package mes.app.plc.service;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import mes.app.plc.dto.PlcHealth;
import mes.app.plc.dto.PlcPoint;
import mes.config.PlcDataSourceConfig.PlcGuard;

/**
 * SY폴리텍 PLC 수집 데이터 조회. 전부 읽기 전용이다.
 *
 * 적재는 수집기(ActasPLC.exe)와 USP_DG110120L10_A10 전담이므로 여기서 쓰지 않는다.
 *
 * 읽는 테이블은 두 개다.
 *   DG2000M  수집 원장(append-only, 약 1,590만 행). 인덱스는 PK 하나뿐이라 조회는 전부 스캔이다.
 *   CD1400M  CD_CLS='DG1010' 접점 마스터 / CD_CLS='DG0900' PLC 접속정보
 *
 * 임계값(DG2010M)은 읽지 않는다. 쓰는 곳이 없어 2026-10-19 에 뺐다. PlcPoint 주석 참조.
 */
@Service
public class PlcMonitorService {

	private static final Logger log = LoggerFactory.getLogger(PlcMonitorService.class);

	/** SY폴리텍. 이 조건이 빠지면 다른 사업장 데이터가 섞인다(F7의 절반). */
	private static final String CORP_CD = "1410";

	/** 접점 마스터 143점은 거의 안 바뀐다. */
	private static final long META_TTL_MS = 10 * 60 * 1000L;

	/** 적재 현황 집계는 1590만 행 스캔이라 2.5초쯤 걸린다. */
	private static final long HEALTH_TTL_MS = 30 * 1000L;

	/** 좁은 구간의 최신값은 1초 미만. 화면이 빠르게 폴링해도 DB를 때리지 않게 짧게 잡는다. */
	private static final long LATEST_TTL_MS = 5 * 1000L;

	/** 이 시간을 넘는 조회 구간은 "마지막 알려진 값"을 보는 용도로 본다(1년). */
	private static final int WIDE_LOOKBACK_HOURS = 8760;

	/** 넓은 구간은 조회가 3.6초쯤 걸리고 값도 과거라 오래 캐시한다. */
	private static final long WIDE_LATEST_TTL_MS = 5 * 60 * 1000L;

	@Autowired
	@Qualifier("plcJdbcTemplate")
	private NamedParameterJdbcTemplate plcJdbc;

	@Autowired
	private PlcGuard guard;

	@Value("${plc.freshness.fallback-period-seconds:60}")
	private long fallbackPeriodSeconds;

	private final Cached<List<Map<String, Object>>> metaCache =
			new Cached<>(META_TTL_MS, this::loadMeta);

	private final Cached<List<Map<String, Object>>> collectorCache =
			new Cached<>(META_TTL_MS, this::loadCollectors);

	private final Cached<List<Map<String, Object>>> healthCache =
			new Cached<>(HEALTH_TTL_MS, this::loadHealthRollup);

	private final Map<Integer, Cached<Map<String, Map<String, Object>>>> latestCache =
			new LinkedHashMap<>();

	/** 화면 셀렉트의 "전체 (마지막 알려진 값)" 값. 워밍업 대상이라 여기서도 안다. */
	private static final int UI_WIDE_LOOKBACK_HOURS = 175200;

	/**
	 * 기동 직후 캐시를 미리 채운다.
	 *
	 * DG2000M 은 인덱스가 없어 콜드 조회가 운영 실측 기준 wide 4초 / health 2초다.
	 * 워밍업이 없으면 첫 화면 클릭이 이걸 직렬로 전부 뒤집어쓴다.
	 * 기동을 막지 않도록 데몬 스레드에서 돌리고, 가드가 잠겨 있으면 건드리지 않는다.
	 */
	@PostConstruct
	void warmUpAsync() {
		Thread t = new Thread(() -> {
			if (!this.guard.isOpen()) {
				log.info("[PLC] 가드가 잠겨 있어 캐시 워밍업을 건너뛴다.");
				return;
			}
			long t0 = System.currentTimeMillis();
			try {
				this.metaCache.get();
				this.collectorCache.get();
				this.healthCache.get();
				latest(UI_WIDE_LOOKBACK_HOURS);
				latest(24);
				log.info("[PLC] 캐시 워밍업 완료 ({}초)", (System.currentTimeMillis() - t0) / 1000.0);
			} catch (Exception e) {
				log.warn("[PLC] 캐시 워밍업 실패. 첫 조회가 느릴 수 있다. ({})", e.getMessage());
			}
		}, "plc-cache-warmup");
		t.setDaemon(true);
		t.start();
	}

	// ------------------------------------------------------------------
	// 공개 API
	// ------------------------------------------------------------------

	/**
	 * 수집기가 살아 있는지. 서버 나머지 코드 없이도 이것만으로 값어치가 있다.
	 */
	public PlcHealth health() {
		this.guard.check();

		LocalDateTime now = LocalDateTime.now();

		PlcHealth out = new PlcHealth();
		out.serverTime = now;
		out.database = this.guard.getDatabase();
		out.collectors = new ArrayList<>();

		List<Map<String, Object>> rollup = this.healthCache.get();

		for (Map<String, Object> c : this.collectorCache.get()) {
			String group = str(c.get("group_cd"));

			PlcHealth.Collector col = new PlcHealth.Collector();
			col.group = group;
			col.ip = str(c.get("ip"));
			col.port = str(c.get("port"));
			col.expectedPointCount = toInt(c.get("point_count"));
			col.cron = str(c.get("cron"));
			col.cyclePeriodSeconds = (Long) c.get("period_seconds");
			col.lines = new ArrayList<>();

			LocalDateTime groupLast = null;

			for (Map<String, Object> r : rollup) {
				if (!group.equals(str(r.get("collector_group")))) {
					continue;
				}
				LocalDateTime last = toDateTime(r.get("last_collected"));

				PlcHealth.Line ln = new PlcHealth.Line();
				ln.line = str(r.get("line_cd"));
				ln.lastCollectedAt = last;
				ln.ageSeconds = ageSeconds(last, now);
				ln.freshness = PlcFreshness.of(ln.ageSeconds, col.cyclePeriodSeconds);
				ln.rowCount = toLong(r.get("row_count"));
				col.lines.add(ln);

				if (last != null && (groupLast == null || last.isAfter(groupLast))) {
					groupLast = last;
				}
			}

			col.lines.sort(Comparator.comparing(l -> l.line));
			col.lastCollectedAt = groupLast;
			col.ageSeconds = ageSeconds(groupLast, now);
			col.freshness = PlcFreshness.of(col.ageSeconds, col.cyclePeriodSeconds);

			out.collectors.add(col);
		}

		out.collectors.sort(Comparator.comparing(c -> c.group));
		out.healthy = !out.collectors.isEmpty()
				&& out.collectors.stream().allMatch(c -> c.freshness == PlcFreshness.FRESH);

		return out;
	}

	/**
	 * 접점 메타 + 최신값. 라인코드로 걸러낼 수 있다.
	 *
	 * @param lineCd        라인코드 4자리. null 이면 전체 143점
	 * @param lookbackHours 이 시간 안에 수집된 값만 최신값으로 인정한다.
	 *                      구간 밖이면 값은 null 이고 freshness 는 DEAD 다.
	 */
	public List<PlcPoint> points(String lineCd, int lookbackHours) {
		this.guard.check();

		LocalDateTime now = LocalDateTime.now();
		Map<String, Map<String, Object>> latest = latest(lookbackHours);
		Map<String, Long> periodByGroup = periodByGroup();
		Map<String, String> groupByLine = groupByLine();

		List<PlcPoint> out = new ArrayList<>();

		for (Map<String, Object> m : this.metaCache.get()) {
			String line = str(m.get("line_cd"));
			if (lineCd != null && !lineCd.equals(line)) {
				continue;
			}

			String id = str(m.get("gath_id"));
			String name = str(m.get("name"));

			PlcPoint p = new PlcPoint();
			p.id = id;
			p.line = line;
			p.seq = toInt(m.get("seq"));
			p.name = name;
			p.kind = PlcKind.fromName(name);
			p.unit = p.kind.getUnit();

			Map<String, Object> row = latest.get(id);
			if (row != null) {
				p.value = toDecimal(row.get("GATH_VAL"));
				p.raw = str(row.get("GATH_DATA"));
				p.collectedAt = toDateTime(row.get("DATA_REGDT"));
				p.collectorGroup = str(row.get("GATH_DESC"));
			} else {
				// 값이 없어도 그룹은 알아야 주기 P 를 붙일 수 있다.
				p.collectorGroup = groupByLine.get(line);
			}

			p.cyclePeriodSeconds = periodByGroup.get(p.collectorGroup);
			p.ageSeconds = ageSeconds(p.collectedAt, now);
			p.freshness = PlcFreshness.of(p.ageSeconds, p.cyclePeriodSeconds);
			// min/max/outOfRange 는 채우지 않는다(항상 null). PlcPoint 주석 참조.

			out.add(p);
		}

		return out;
	}

	/**
	 * 호기 번호로 접점을 찾는다.
	 *
	 * ★ MES 호기와 PLC 라인코드를 잇는 마스터가 없어서, 접점 명칭(CD_NM)의 "N호기" 문자열로
	 * 잠정 연결한다. kind 를 CD_NM 에서 도출하는 것(§4-3)과 같은 방식이자 같은 한계다.
	 * PLC 세션 확인 전까지 화면에 "잠정"으로 표시해야 한다.
	 *
	 * 이름에 호기 번호가 없는 접점은 잡히지 않는다 —
	 * 1052/1054(1·2라인 친환경), 1100(창고·라인 온습도), 1110(생산 진행 확인·호퍼온도)이 그렇다.
	 */
	public List<PlcPoint> pointsForMachineNo(int machineNo, int lookbackHours) {
		return points(null, lookbackHours).stream()
				.filter(p -> {
					Integer no = MachineNo.parse(p.name);
					return no != null && no == machineNo;
				})
				.toList();
	}

	/** 단건 조회 */
	public PlcPoint point(String gathId, int lookbackHours) {
		return points(null, lookbackHours).stream()
				.filter(p -> p.id.equals(gathId))
				.findFirst()
				.orElse(null);
	}

	/** 화면에 걸 라인 목록(라인코드 + 접점수) */
	public List<Map<String, Object>> lines() {
		this.guard.check();

		Map<String, String> groupByLine = groupByLine();
		Map<String, Integer> counts = new LinkedHashMap<>();

		for (Map<String, Object> m : this.metaCache.get()) {
			counts.merge(str(m.get("line_cd")), 1, Integer::sum);
		}

		List<Map<String, Object>> out = new ArrayList<>();
		counts.forEach((line, cnt) -> {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("line", line);
			row.put("pointCount", cnt);
			row.put("collectorGroup", groupByLine.get(line));
			out.add(row);
		});
		return out;
	}

	/** 기간 조회. 인덱스가 없어 전체 스캔이므로 행수를 반드시 제한한다. */
	public List<Map<String, Object>> history(String gathId, LocalDateTime from, LocalDateTime to, int limit) {
		this.guard.check();

		String sql = """
				SELECT TOP (:limit) GATH_ID, GATH_IDX, GATH_VAL, GATH_DATA, DATA_REGDT
				FROM   DG2000M
				WHERE  CORP_CD = :corpCd
				AND    GATH_ID = :gathId
				AND    DATA_REGDT >= :fromDt
				AND    DATA_REGDT <  :toDt
				ORDER BY GATH_IDX
				""";

		MapSqlParameterSource param = new MapSqlParameterSource()
				.addValue("limit", limit)
				.addValue("corpCd", CORP_CD)
				.addValue("gathId", gathId)
				.addValue("fromDt", Timestamp.valueOf(from))
				.addValue("toDt", Timestamp.valueOf(to));

		return this.plcJdbc.queryForList(sql, param);
	}

	// ------------------------------------------------------------------
	// 조회
	// ------------------------------------------------------------------

	private List<Map<String, Object>> loadMeta() {
		String sql = """
				SELECT CD AS gath_id, CD_V1 AS line_cd, DSP_SEQ AS seq, CD_NM AS name
				FROM   CD1400M
				WHERE  CORP_CD = :corpCd
				AND    CORP_CLS = '10'
				AND    CD_CLS = 'DG1010'
				AND    EFF_ENDT = '9999-12-31'
				ORDER BY CD_V1, DSP_SEQ
				""";
		return this.plcJdbc.queryForList(sql, new MapSqlParameterSource("corpCd", CORP_CD));
	}

	private List<Map<String, Object>> loadCollectors() {
		String sql = """
				SELECT CD AS group_cd, CD_V1 AS ip, CD_V2 AS port,
				       CD_V3 AS start_addr, CD_V4 AS point_count, CD_V7 AS cron
				FROM   CD1400M
				WHERE  CORP_CD = :corpCd
				AND    CD_CLS = 'DG0900'
				AND    EFF_ENDT = '9999-12-31'
				ORDER BY CD
				""";
		List<Map<String, Object>> rows =
				this.plcJdbc.queryForList(sql, new MapSqlParameterSource("corpCd", CORP_CD));

		for (Map<String, Object> r : rows) {
			r.put("period_seconds", periodFromCron(str(r.get("cron"))));
		}
		return rows;
	}

	/**
	 * 그룹·라인별 마지막 적재 시각. 한 번 스캔으로 /health 와 라인→그룹 매핑을 함께 얻는다.
	 */
	private List<Map<String, Object>> loadHealthRollup() {
		String sql = """
				SELECT GATH_DESC AS collector_group,
				       EQIP_ID   AS line_cd,
				       COUNT(*)  AS row_count,
				       MAX(DATA_REGDT) AS last_collected
				FROM   DG2000M
				WHERE  CORP_CD = :corpCd
				GROUP BY GATH_DESC, EQIP_ID
				""";
		return this.plcJdbc.queryForList(sql, new MapSqlParameterSource("corpCd", CORP_CD));
	}

	/**
	 * 접점별 최신값. 인수인계서 §4-1 계약 그대로다.
	 *
	 * 정렬은 반드시 GATH_IDX DESC 여야 한다. DATA_REGDT 는 smalldatetime 이라 분 해상도이고,
	 * 수집 주기가 1분이면 같은 분의 행이 전부 동률이 되어 "최신 1건"이 비결정적이 된다.
	 * GATH_IDX 는 IDENTITY 라 단조 증가하므로 유일하게 안전하다.
	 * DATA_REGDT 는 표시와 신선도 판정에만 쓴다.
	 *
	 * CORP_CD 조건과 시간 하한은 둘 다 필수다. 이 둘이 없던 것이 기존 대시보드의 F7 이고,
	 * 수집이 끊겨도 옛날 값을 정상값처럼 보여준 원인이다.
	 *
	 * GATH_YM 으로는 거르지 않는다. 이 컬럼은 포맷이 두 가지다.
	 * 그룹 '20' 시절 약 67만 행이 'YYYY-MM' 이고 나머지 약 1523만 행이 'YYYY-MM-DD' 라,
	 * 문자열 범위 조건을 걸면 그룹 '20' 이 통째로 조용히 빠진다.
	 */
	private Map<String, Map<String, Object>> latest(int lookbackHours) {
		Cached<Map<String, Map<String, Object>>> cached;
		synchronized (this.latestCache) {
			// 넓은 구간은 조회가 3초 넘게 걸리는데다 이미 지나간 값이라 자주 다시 읽을 이유가 없다.
			// 좁은 구간은 실시간성이 중요하므로 짧게 잡는다.
			long ttl = (lookbackHours > WIDE_LOOKBACK_HOURS) ? WIDE_LATEST_TTL_MS : LATEST_TTL_MS;
			cached = this.latestCache.computeIfAbsent(lookbackHours,
					h -> new Cached<>(ttl, () -> loadLatest(h)));
		}
		return cached.get();
	}

	private Map<String, Map<String, Object>> loadLatest(int lookbackHours) {
		// 계약(§4-1)은 ROW_NUMBER() OVER (ORDER BY GATH_IDX DESC) 형태로 적혀 있으나
		// 여기서는 접점별 MAX(GATH_IDX) 집계 후 되짚는 형태로 쓴다. 결과는 동일하다 —
		// GATH_IDX 가 IDENTITY 라 접점별 최대값이 곧 최신 1건이고, 정렬 키 규칙도 그대로다.
		//
		// 바꾼 이유는 성능이다. 조회 구간을 넓게 잡으면 ROW_NUMBER 는 1,590만 행을 정렬해야 해서
		// 20.5초가 걸린다(실측). 해시 집계로 바꾸면 같은 구간이 3.6초다.
		// 좁은 구간에서는 시간 조건이 행을 다 걸러내 어느 쪽이든 1초 미만이다.
		//
		// CORP_CD 조건과 시간 하한은 둘 다 그대로 유지한다. 이 둘이 F7 의 원인이었다.
		String sql = """
				SELECT A.GATH_ID, A.GATH_VAL, A.GATH_DATA, A.EQIP_ID, A.GATH_DESC, A.DATA_REGDT
				FROM   DG2000M A
				JOIN  (SELECT   GATH_ID, MAX(GATH_IDX) AS MX
				       FROM     DG2000M
				       WHERE    CORP_CD = :corpCd
				       AND      DATA_REGDT >= DATEADD(HOUR, :lookbackHours, GETDATE())
				       GROUP BY GATH_ID) M
				  ON   M.GATH_ID = A.GATH_ID
				 AND   M.MX      = A.GATH_IDX
				WHERE  A.CORP_CD = :corpCd
				""";

		MapSqlParameterSource param = new MapSqlParameterSource()
				.addValue("corpCd", CORP_CD)
				// 음수는 SQL 쪽에서 -? 를 쓰지 않도록 여기서 만든다.
				.addValue("lookbackHours", -Math.abs(lookbackHours));

		Map<String, Map<String, Object>> out = new LinkedHashMap<>();
		for (Map<String, Object> r : this.plcJdbc.queryForList(sql, param)) {
			out.put(str(r.get("GATH_ID")), r);
		}
		return out;
	}

	// ------------------------------------------------------------------
	// 파생값
	// ------------------------------------------------------------------

	private Map<String, Long> periodByGroup() {
		Map<String, Long> out = new LinkedHashMap<>();
		for (Map<String, Object> c : this.collectorCache.get()) {
			out.put(str(c.get("group_cd")), (Long) c.get("period_seconds"));
		}
		return out;
	}

	/**
	 * 라인코드 → 수집기 그룹. 적재 현황 집계에서 끌어온다.
	 * 그룹이 빈 행은 2023-06 시절 잔재라 버린다.
	 */
	private Map<String, String> groupByLine() {
		Map<String, String> out = new LinkedHashMap<>();
		for (Map<String, Object> r : this.healthCache.get()) {
			String group = str(r.get("collector_group"));
			if (group == null || group.isBlank()) {
				continue;
			}
			out.put(str(r.get("line_cd")), group);
		}
		return out;
	}

	/**
	 * 크론에서 수집 주기 P(초)를 산출한다. 하드코딩하지 않는다.
	 * 연속한 두 발화 시각의 간격을 잰다. 예) '0 0/1 * * * ?' -> 60
	 */
	private Long periodFromCron(String cron) {
		if (cron == null || cron.isBlank()) {
			return this.fallbackPeriodSeconds;
		}
		try {
			CronExpression expr = CronExpression.parse(cron.trim());
			LocalDateTime base = LocalDateTime.now().withNano(0);
			LocalDateTime n1 = expr.next(base);
			LocalDateTime n2 = (n1 == null) ? null : expr.next(n1);
			if (n1 == null || n2 == null) {
				return this.fallbackPeriodSeconds;
			}
			long seconds = Duration.between(n1, n2).getSeconds();
			return seconds > 0 ? seconds : this.fallbackPeriodSeconds;
		} catch (Exception e) {
			log.warn("[PLC] 수집주기 크론 해석 실패 '{}'. 기본값 {}초를 쓴다.", cron, this.fallbackPeriodSeconds);
			return this.fallbackPeriodSeconds;
		}
	}

	/**
	 * DATA_REGDT 는 smalldatetime 이라 30초 이상이 다음 분으로 반올림된다.
	 * 그래서 경과 시간이 최대 -30초까지 음수로 나올 수 있다. 0으로 클램프한다.
	 */
	private static Long ageSeconds(LocalDateTime collectedAt, LocalDateTime now) {
		if (collectedAt == null) {
			return null;
		}
		return Math.max(0, Duration.between(collectedAt, now).getSeconds());
	}

	// ------------------------------------------------------------------
	// 변환 도우미
	// ------------------------------------------------------------------

	private static String str(Object o) {
		return (o == null) ? null : o.toString().trim();
	}

	// CD1400M 의 CD_V*, DSP_SEQ 는 nvarchar 라 숫자여도 String 으로 올라온다.
	private static Integer toInt(Object o) {
		Long v = toLong(o);
		return (v == null) ? null : v.intValue();
	}

	private static Long toLong(Object o) {
		if (o instanceof Number n) {
			return n.longValue();
		}
		if (o instanceof CharSequence s) {
			try {
				return Long.parseLong(s.toString().trim());
			} catch (NumberFormatException e) {
				return null;
			}
		}
		return null;
	}

	private static BigDecimal toDecimal(Object o) {
		if (o instanceof BigDecimal b) {
			return b;
		}
		return (o instanceof Number n) ? BigDecimal.valueOf(n.doubleValue()) : null;
	}

	private static LocalDateTime toDateTime(Object o) {
		if (o instanceof Timestamp t) {
			return t.toLocalDateTime();
		}
		return (o instanceof LocalDateTime d) ? d : null;
	}

	/**
	 * TTL 캐시 — stale-while-revalidate.
	 *
	 * 만료되어도 호출자를 기다리게 하지 않는다. 이전 값을 즉시 돌려주고 갱신은 백그라운드
	 * 스레드 하나가 한다. DG2000M 조회가 2~4초짜리 풀스캔이라(운영 실측),
	 * 만료 시점에 걸린 사용자가 그 스캔을 통째로 기다리던 것이 화면이 수십 초 밀리던 원인이다.
	 *
	 * 값이 아예 없을 때(기동 직후 워밍업 전)만 동기로 읽는다. 백그라운드 갱신이 실패하면
	 * 이전 값을 유지한다 — PLC 모니터링에서 몇 초 묵은 값이 빈 화면보다 낫다.
	 * 갱신 스레드를 하나로 묶어 커넥션 풀(최대 4)을 캐시 갱신이 점령하지 못하게 한다.
	 */
	private static final class Cached<T> {

		private static final Logger cacheLog = LoggerFactory.getLogger(Cached.class);

		/** 모든 캐시가 공유하는 갱신 스레드. 데몬이라 종료를 막지 않는다. */
		private static final ExecutorService REFRESHER = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "plc-cache-refresh");
			t.setDaemon(true);
			return t;
		});

		private final long ttlMillis;
		private final Supplier<T> loader;
		private volatile T value;
		private volatile long loadedAt = 0L;
		private final AtomicBoolean refreshing = new AtomicBoolean(false);

		Cached(long ttlMillis, Supplier<T> loader) {
			this.ttlMillis = ttlMillis;
			this.loader = loader;
		}

		T get() {
			T v = this.value;
			if (v != null) {
				if (System.currentTimeMillis() - this.loadedAt >= this.ttlMillis) {
					refreshAsync();
				}
				return v;   // 만료됐어도 일단 이전 값. 갱신은 백그라운드가 한다.
			}
			synchronized (this) {
				if (this.value == null) {
					this.value = this.loader.get();
					this.loadedAt = System.currentTimeMillis();
				}
				return this.value;
			}
		}

		private void refreshAsync() {
			if (!this.refreshing.compareAndSet(false, true)) {
				return;   // 이미 갱신 중
			}
			REFRESHER.submit(() -> {
				try {
					T fresh = this.loader.get();
					this.value = fresh;
					this.loadedAt = System.currentTimeMillis();
				} catch (Exception e) {
					// 이전 값을 유지한다. 다음 get() 이 다시 갱신을 건다.
					cacheLog.warn("[PLC] 캐시 백그라운드 갱신 실패. 이전 값을 유지한다. ({})", e.getMessage());
				} finally {
					this.refreshing.set(false);
				}
			});
		}
	}
}
