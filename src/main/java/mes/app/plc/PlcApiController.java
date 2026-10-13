package mes.app.plc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.plc.dto.PlcHealth;
import mes.app.plc.dto.PlcPoint;
import mes.app.plc.service.PlcMonitorService;

/**
 * SY폴리텍 PLC 모니터링 조회 API. 전부 읽기 전용이다. 쓰기 엔드포인트는 만들지 않는다.
 *
 * 기존 WinForms 계열이 쓰는 WAS(http://121.140.143.36:1000/ 의 POST api/Proc)와는 별개다.
 *
 * 응답은 세로로 내보낸다. 기존 프로시저 22개처럼
 * AREA_1_TP_VALUE | AREA_2_TP_VALUE 로 펼치면 화면이 컬럼명에 묶인다.
 */
@RestController
@RequestMapping("/api/plc")
public class PlcApiController {

	/** 최신값으로 인정할 기본 조회 구간(시간). */
	private static final int DEFAULT_LOOKBACK_HOURS = 24;

	/** 기간 조회 상한. DG2000M 은 인덱스가 PK 하나뿐이라 무제한 조회를 열어두면 안 된다. */
	private static final int MAX_HISTORY_ROWS = 5000;

	@Autowired
	private PlcMonitorService plcMonitorService;

	/**
	 * 수집기가 살아 있는지. 가장 먼저 봐야 할 화면이다.
	 *
	 * 지금은 두 장비 다 DEAD 로 나오는 게 정상이다.
	 * 그룹 '10' 은 2025-12-11, 그룹 '20' 은 2023-12-29 이후로 적재가 없다.
	 */
	@GetMapping("/health")
	public PlcHealth health() {
		return this.plcMonitorService.health();
	}

	/** 접점 메타 + 최신값. 라인코드로 걸러낼 수 있다. */
	@GetMapping("/points")
	public List<PlcPoint> points(
			@RequestParam(value = "line", required = false) String line,
			@RequestParam(value = "lookbackHours", required = false) Integer lookbackHours) {

		return this.plcMonitorService.points(line, lookback(lookbackHours));
	}

	/** 단건 최신값 + 임계값 + 신선도 */
	@GetMapping("/points/{gathId}")
	public ResponseEntity<PlcPoint> point(
			@PathVariable("gathId") String gathId,
			@RequestParam(value = "lookbackHours", required = false) Integer lookbackHours) {

		PlcPoint point = this.plcMonitorService.point(gathId, lookback(lookbackHours));
		return (point == null) ? ResponseEntity.notFound().build() : ResponseEntity.ok(point);
	}

	/** 라인 목록 */
	@GetMapping("/lines")
	public List<Map<String, Object>> lines() {
		return this.plcMonitorService.lines();
	}

	/** 라인 단위 묶음. 화면 하나가 호출 한 번으로 끝난다. */
	@GetMapping("/lines/{lineCd}/latest")
	public Map<String, Object> lineLatest(
			@PathVariable("lineCd") String lineCd,
			@RequestParam(value = "lookbackHours", required = false) Integer lookbackHours) {

		List<PlcPoint> points = this.plcMonitorService.points(lineCd, lookback(lookbackHours));

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("line", lineCd);
		out.put("points", points);
		return out;
	}

	/** 기간 조회 */
	@GetMapping("/history")
	public Map<String, Object> history(
			@RequestParam("gathId") String gathId,
			@RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
			@RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
			@RequestParam(value = "limit", required = false) Integer limit) {

		int capped = (limit == null || limit <= 0) ? MAX_HISTORY_ROWS : Math.min(limit, MAX_HISTORY_ROWS);

		List<Map<String, Object>> rows = this.plcMonitorService.history(
				gathId, from.atStartOfDay(), to.plusDays(1).atStartOfDay(), capped);

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("gathId", gathId);
		out.put("from", from);
		out.put("to", to);
		out.put("limit", capped);
		out.put("truncated", rows.size() >= capped);
		out.put("rows", rows);
		return out;
	}

	/**
	 * 수집 DB 접속이 확인되지 않은 상태. 붙지 못했거나 접속 대상이 FactMES_SYP_TEST 가 아니다.
	 * 화면이 빈 값을 정상값으로 오해하지 않도록 200 이 아니라 503 으로 돌려준다.
	 */
	@ExceptionHandler(IllegalStateException.class)
	public ResponseEntity<Map<String, Object>> handleGuard(IllegalStateException e) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("error", "PLC_DB_UNAVAILABLE");
		body.put("message", e.getMessage());
		body.put("serverTime", LocalDateTime.now());
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
	}

	private static int lookback(Integer requested) {
		return (requested == null || requested <= 0) ? DEFAULT_LOOKBACK_HOURS : requested;
	}
}
