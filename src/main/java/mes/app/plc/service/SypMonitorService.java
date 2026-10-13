package mes.app.plc.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import mes.app.plc.dto.SypMachine;
import mes.app.plc.dto.SypPdirRow;
import mes.config.PlcDataSourceConfig.PlcGuard;

/**
 * SY폴리텍 현장 모니터링 — 호기 / 작업지시 현황 조회. 전부 읽기 전용이다.
 *
 * PLC 접점 데이터와 같은 DB(FactMES_SYP_TEST)에 있어 DataSource 를 공유한다.
 * PLC 쪽이 2025-12-11 이후 수집 중단인 것과 달리 이쪽은 현행 데이터다.
 *
 * 산식은 MES_SYPOLYTECH 세션과 합의한 계약이다.
 * 원형 프로시저 USP_PDIR_MONITORING 은 호출하지 않는다 — 조회일자가 '2023-06-14' 상수로
 * 박혀 있고 @V_EQUI_CD 를 선언만 하고 WHERE 에 쓰지 않아 호기를 바꿔도 같은 결과가 나오며,
 * WinForms 화면이 같은 프로시저를 물고 있어 고치면 부수효과가 난다.
 */
@Service
public class SypMonitorService {

	/** SY폴리텍 */
	private static final String CORP_CD = "1410";

	/** PM1100M / PM1200M 모두 이 값 단일이다(실측). */
	private static final String CORP_CLS = "10";

	/** 믹서 공정. 호기 판별과 작업배치 집계가 모두 이 공정 기준이다. */
	private static final String PROC_MIXER = "1000";

	/**
	 * 지시구분 코드그룹.
	 * ★ 'PM1020'(10 자동 / 20 수작업)이 아니라 'PD3042'(10 정상 / 20 재작업 / 30 샘플 / 40 소진)다.
	 * 두 그룹이 같은 코드값에 다른 의미를 담고 있어 잘못 조인해도 에러 없이 그럴듯한 라벨이 나온다.
	 * 기존 MES 운영 화면(USP_PM1100L10)이 PM1020 으로 조인해 지금 틀린 라벨을 표시하고 있다.
	 */
	private static final String CD_CLS_PDIR = "PD3042";

	private static final String EFF_END = "9999-12-31";

	@Autowired
	@Qualifier("plcJdbcTemplate")
	private NamedParameterJdbcTemplate jdbc;

	@Autowired
	private PlcGuard guard;

	// ------------------------------------------------------------------
	// 호기
	// ------------------------------------------------------------------

	/**
	 * 화면 대상 호기 목록.
	 *
	 * PROC_CD='1000'(믹서) + 사용중 + 미삭제 = 12건.
	 * 정렬은 order_num 이다. EQUI_CD 로 정렬하면 호기 번호 순서가 뒤섞인다.
	 */
	public List<SypMachine> machines() {
		this.guard.check();

		String sql = """
				SELECT EQUI_CD, EQUI_NM, LANE_CD, order_num
				FROM   EM1000M
				WHERE  CORP_CD  = :corpCd
				AND    PROC_CD  = :procCd
				AND    USE_YN   = 'Y'
				AND    ISNULL(DEL_YN, 'N') <> 'Y'
				ORDER BY order_num, EQUI_CD
				""";

		MapSqlParameterSource param = new MapSqlParameterSource()
				.addValue("corpCd", CORP_CD)
				.addValue("procCd", PROC_MIXER);

		List<SypMachine> out = new ArrayList<>();
		for (Map<String, Object> r : this.jdbc.queryForList(sql, param)) {
			SypMachine m = new SypMachine();
			m.equiCd = str(r.get("EQUI_CD"));
			m.equiNm = str(r.get("EQUI_NM"));
			m.laneCd = str(r.get("LANE_CD"));
			m.orderNum = toInt(r.get("order_num"));
			m.machineNo = MachineNo.parse(m.equiNm);
			// statusName 은 판정 근거가 없어 비워 둔다. SypMachine 주석 참조.
			out.add(m);
		}
		return out;
	}

	/** 지시가 존재하는 가장 최근 지시일자. 오늘 지시가 없을 때 화면이 안내하려고 쓴다. */
	public String latestPdirDate() {
		this.guard.check();

		String sql = """
				SELECT MAX(PDIR_DT) AS last_dt
				FROM   PM1100M
				WHERE  CORP_CD = :corpCd
				AND    ISNULL(DEL_YN, 'N') <> 'Y'
				""";
		Map<String, Object> row = this.jdbc.queryForMap(sql, new MapSqlParameterSource("corpCd", CORP_CD));
		return str(row.get("last_dt"));
	}

	// ------------------------------------------------------------------
	// 작업지시
	// ------------------------------------------------------------------

	/**
	 * 호기 하나의 작업지시 현황.
	 *
	 * @param equiCd 믹서 설비코드. null 이면 전 호기
	 * @param pdirDt 지시일자 'yyyy-MM-dd'. null 이면 일자 조건 없음
	 */
	public List<SypPdirRow> pdirRows(String equiCd, String pdirDt) {
		this.guard.check();

		// CD1400M 조인에 CORP_CD/CORP_CLS 를 모두 건다.
		// 기존 MES 프로시저들은 CD_CLS + CD 둘로만 조인하고 있어 다회사 데이터가 들어오면
		// 행이 배수로 늘어난다. 원형을 베끼면 그 결함까지 따라온다.
		String sql = """
				SELECT  p.PDIR_NO, p.PDIR_SQ, p.PDIR_DT, p.EQUI_CD,
				        p.ITEM_CD, p.PDIR_WT, p.BATCH_QTY, p.BATCH_WT,
				        p.COM_YN, p.PDIR_CLS, p.ALS_YN,
				        v.VEND_NM, d.ITEM_NM, d.ITEM_SIZE,
				        cls.CD_NM AS PDIR_CLS_NM,
				        ISNULL(b.BATCH_CNT, 0) AS WORK_BATCH_QTY
				FROM    PM1100M p
				LEFT JOIN (
				        SELECT   PDIR_NO, COUNT(1) AS BATCH_CNT
				        FROM     PM1200M
				        WHERE    CORP_CD  = :corpCd
				        AND      CORP_CLS = :corpCls
				        AND      PROC_CD  = :procCd
				        AND      HALF_YN  = 'N'
				        AND      DEL_YN   = 'N'
				        GROUP BY PDIR_NO
				) b   ON b.PDIR_NO = p.PDIR_NO
				LEFT JOIN PD1000M d
				       ON d.CORP_CD  = p.CORP_CD
				      AND d.CORP_CLS = p.CORP_CLS
				      AND d.ITEM_CD  = p.ITEM_CD
				LEFT JOIN CD1300M v
				       ON v.CORP_CD  = p.CORP_CD
				      AND v.CORP_CLS = p.CORP_CLS
				      AND v.VEND_NO  = p.VEND_NO
				LEFT JOIN CD1400M cls
				       ON cls.CORP_CD  = p.CORP_CD
				      AND cls.CORP_CLS = p.CORP_CLS
				      AND cls.CD_CLS   = :cdClsPdir
				      AND cls.CD       = p.PDIR_CLS
				      AND cls.EFF_ENDT = :effEnd
				WHERE   p.CORP_CD = :corpCd
				AND     ISNULL(p.DEL_YN, 'N') <> 'Y'
				AND     (:equiCd IS NULL OR p.EQUI_CD = :equiCd)
				AND     (:pdirDt IS NULL OR p.PDIR_DT = :pdirDt)
				ORDER BY p.EQUI_CD, p.PDIR_NO, p.PDIR_SQ
				""";

		MapSqlParameterSource param = new MapSqlParameterSource()
				.addValue("corpCd", CORP_CD)
				.addValue("corpCls", CORP_CLS)
				.addValue("procCd", PROC_MIXER)
				.addValue("cdClsPdir", CD_CLS_PDIR)
				.addValue("effEnd", EFF_END)
				// 널 파라미터는 타입을 명시한다. 안 그러면 SQL Server 가 타입을 못 정해 실패한다.
				.addValue("equiCd", equiCd, Types.NVARCHAR)
				.addValue("pdirDt", pdirDt, Types.NVARCHAR);

		List<SypPdirRow> out = new ArrayList<>();
		for (Map<String, Object> r : this.jdbc.queryForList(sql, param)) {
			out.add(toRow(r));
		}
		return out;
	}

	/**
	 * 이 작업지시의 최신 배치 한 건.
	 *
	 * PM1200M 은 배치마다 한 행이고 PSQ 가 배치 순번이다.
	 * WORK_TM 이 배치 시작시각, WORK_RUN 이 완료시각이며 둘 다 datetime(초까지)이다.
	 * ★ WORK_RUN 이 비어 있으면 그 배치가 지금 돌고 있는 것이다.
	 *
	 * 레이아웃의 "믹서시간"은 배치 소요시간이다(목업 시작 12:00 / 완료 12:35 / 35분).
	 * 진행중이면 완료시각이 없으므로 시작시각부터 지금까지의 경과로 낸다.
	 */
	public Map<String, Object> currentBatch(String pdirNo) {
		this.guard.check();

		String sql = """
				SELECT TOP 1 PSQ, WORK_DT, WORK_TM, WORK_RUN
				FROM   PM1200M
				WHERE  CORP_CD  = :corpCd
				AND    CORP_CLS = :corpCls
				AND    PDIR_NO  = :pdirNo
				AND    PROC_CD  = :procCd
				AND    HALF_YN  = 'N'
				AND    DEL_YN   = 'N'
				ORDER BY PSQ DESC
				""";

		MapSqlParameterSource param = new MapSqlParameterSource()
				.addValue("corpCd", CORP_CD)
				.addValue("corpCls", CORP_CLS)
				.addValue("procCd", PROC_MIXER)
				.addValue("pdirNo", pdirNo);

		List<Map<String, Object>> rows = this.jdbc.queryForList(sql, param);
		if (rows.isEmpty()) {
			return null;
		}
		Map<String, Object> r = rows.get(0);

		LocalDateTime start = dt(r.get("WORK_TM"));
		LocalDateTime end = dt(r.get("WORK_RUN"));
		boolean running = (start != null && end == null);

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("psq", toInt(r.get("PSQ")));
		out.put("workDt", str(r.get("WORK_DT")));
		out.put("startedAt", start);
		out.put("finishedAt", end);
		out.put("running", running);

		// 진행중이면 지금까지, 끝났으면 실제 소요시간
		LocalDateTime until = (end != null) ? end : LocalDateTime.now();
		out.put("elapsedSeconds",
				(start == null) ? null : Math.max(0, Duration.between(start, until).getSeconds()));

		return out;
	}

	/** 호기 목록 + 각 호기의 지시 건수 요약을 한 번에. 화면 진입 시 1콜로 끝내려는 용도다. */
	public Map<String, Object> overview(String pdirDt) {
		List<SypMachine> machines = machines();
		List<SypPdirRow> rows = pdirRows(null, pdirDt);

		Map<String, SypMachine> byEqui = new LinkedHashMap<>();
		for (SypMachine m : machines) {
			byEqui.put(m.equiCd, m);
		}

		for (SypPdirRow r : rows) {
			SypMachine m = byEqui.get(r.equiCd);
			if (m == null) {
				continue;   // 폐기 설비에 걸린 과거 지시
			}
			m.totalCount++;
			switch (r.statusCode) {
				case "DONE" -> m.doneCount++;
				case "RUNNING" -> m.runningCount++;
				default -> m.waitingCount++;
			}
		}

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("pdirDt", pdirDt);
		out.put("latestPdirDt", latestPdirDate());
		out.put("machines", machines);
		out.put("rows", rows);
		return out;
	}

	// ------------------------------------------------------------------
	// 매핑
	// ------------------------------------------------------------------

	private static SypPdirRow toRow(Map<String, Object> r) {
		SypPdirRow p = new SypPdirRow();
		p.pdirNo = str(r.get("PDIR_NO"));
		p.pdirSq = toInt(r.get("PDIR_SQ"));
		p.pdirDt = str(r.get("PDIR_DT"));
		p.equiCd = str(r.get("EQUI_CD"));
		p.itemCd = str(r.get("ITEM_CD"));
		p.vendNm = str(r.get("VEND_NM"));
		p.itemNm = str(r.get("ITEM_NM"));
		p.itemSize = str(r.get("ITEM_SIZE"));
		p.pdirWt = dec(r.get("PDIR_WT"));
		p.batchQty = dec(r.get("BATCH_QTY"));
		p.batchWt = dec(r.get("BATCH_WT"));
		p.workBatchQty = dec(r.get("WORK_BATCH_QTY"));
		p.comYn = str(r.get("COM_YN"));
		p.pdirCls = str(r.get("PDIR_CLS"));
		p.pdirClsNm = str(r.get("PDIR_CLS_NM"));
		p.alsYn = str(r.get("ALS_YN"));

		// 생산량 = 작업Batch × 배치당 중량
		if (p.workBatchQty != null && p.batchWt != null) {
			p.producedWt = p.workBatchQty.multiply(p.batchWt);
		}

		if (p.producedWt != null && p.pdirWt != null && p.pdirWt.signum() != 0) {
			p.progressRate = p.producedWt
					.multiply(BigDecimal.valueOf(100))
					.divide(p.pdirWt, 1, RoundingMode.HALF_UP);
		}

		// 완료 → 작업Batch 있으면 진행중 → 아니면 대기
		boolean done = "Y".equalsIgnoreCase(p.comYn);
		boolean started = p.workBatchQty != null && p.workBatchQty.signum() > 0;
		if (done) {
			p.statusCode = "DONE";
			p.statusName = "작업완료";
		} else if (started) {
			p.statusCode = "RUNNING";
			p.statusName = "진행중";
		} else {
			p.statusCode = "WAITING";
			p.statusName = "대기";
		}

		return p;
	}

	private static String str(Object o) {
		if (o == null) {
			return null;
		}
		String s = o.toString().trim();
		return s.isEmpty() ? null : s;
	}

	private static Integer toInt(Object o) {
		if (o instanceof Number n) {
			return n.intValue();
		}
		if (o instanceof CharSequence s) {
			try {
				return Integer.valueOf(s.toString().trim());
			} catch (NumberFormatException e) {
				return null;
			}
		}
		return null;
	}

	private static LocalDateTime dt(Object o) {
		if (o instanceof Timestamp t) {
			return t.toLocalDateTime();
		}
		return (o instanceof LocalDateTime d) ? d : null;
	}

	private static BigDecimal dec(Object o) {
		if (o instanceof BigDecimal b) {
			return b;
		}
		if (o instanceof Number n) {
			return BigDecimal.valueOf(n.doubleValue());
		}
		return null;
	}
}
