package mes.app.plc;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.plc.dto.SypMachine;
import mes.app.plc.dto.SypPdirRow;
import mes.app.plc.dto.SypZoneTemp;
import mes.app.plc.service.PlcMonitorService;
import mes.app.plc.service.SypMonitorService;

/**
 * SY폴리텍 현장 모니터링 — 호기 / 작업지시 조회 API. 읽기 전용이다.
 *
 * PLC 접점 API(/api/plc/**)와 같은 보안 체인에 속한다(PlcSecurityConfig).
 * 경로를 나눈 이유는 데이터 출처가 달라서다 — 이쪽은 MES 업무 데이터,
 * 저쪽은 PLC 수집 접점이다. 다원유 MES 의 /api/** 와 겹치지 않게 syp 로 분리했다.
 */
@RestController
@RequestMapping("/api/syp")
public class SypApiController {

	private static final DateTimeFormatter DT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	/** PLC 최신값으로 인정할 기본 구간(시간). */
	private static final int DEFAULT_LOOKBACK_HOURS = 24;

	@Autowired
	private SypMonitorService sypMonitorService;

	@Autowired
	private PlcMonitorService plcMonitorService;

	/** 화면 대상 호기 12건 */
	@GetMapping("/machines")
	public List<SypMachine> machines() {
		return this.sypMonitorService.machines();
	}

	/**
	 * 호기 목록 + 지시 목록을 한 번에. 화면 진입은 이걸 한 번 부르면 된다.
	 *
	 * @param pdirDt 지시일자 'yyyy-MM-dd'. 생략하면 오늘.
	 *               오늘 지시가 없을 수 있어 응답에 latestPdirDt 를 같이 담는다.
	 */
	@GetMapping("/overview")
	public Map<String, Object> overview(
			@RequestParam(value = "pdirDt", required = false) String pdirDt) {

		return this.sypMonitorService.overview(resolveDate(pdirDt));
	}

	/** 호기 하나의 작업지시 현황 */
	@GetMapping("/pdir")
	public Map<String, Object> pdir(
			@RequestParam(value = "equiCd", required = false) String equiCd,
			@RequestParam(value = "pdirDt", required = false) String pdirDt) {

		String date = resolveDate(pdirDt);
		List<SypPdirRow> rows = this.sypMonitorService.pdirRows(equiCd, date);

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("equiCd", equiCd);
		out.put("pdirDt", date);
		out.put("rows", rows);
		return out;
	}

	/**
	 * 레이아웃 하단 "설비 현황". 선택된 작업지시 1건 + 그 배치 + 해당 호기의 PLC 접점.
	 *
	 * 지시를 지정하지 않으면 그 호기의 진행중 지시를 자동으로 고른다
	 * (한 설비에 진행중 지시가 둘일 수 없다). 진행중이 없으면 가장 최근 완료 건을 쓴다.
	 *
	 * @param lookbackHours PLC 최신값으로 인정할 구간. 수집이 멈춰 있어 기본 24시간이면 전부 비는데,
	 *                      그게 정상 동작이다. 마지막 알려진 값을 보려면 크게 준다.
	 */
	@GetMapping("/equipment")
	public Map<String, Object> equipment(
			@RequestParam("equiCd") String equiCd,
			@RequestParam(value = "pdirNo", required = false) String pdirNo,
			@RequestParam(value = "pdirDt", required = false) String pdirDt,
			@RequestParam(value = "lookbackHours", required = false) Integer lookbackHours) {

		String date = resolveDate(pdirDt);

		SypMachine machine = this.sypMonitorService.machines().stream()
				.filter(m -> equiCd.equals(m.equiCd))
				.findFirst().orElse(null);

		List<SypPdirRow> rows = this.sypMonitorService.pdirRows(equiCd, date);

		// 지정된 지시 > 진행중 > 마지막 건
		SypPdirRow picked = null;
		if (pdirNo != null && !pdirNo.isBlank()) {
			picked = rows.stream().filter(r -> pdirNo.equals(r.pdirNo)).findFirst().orElse(null);
		}
		if (picked == null) {
			picked = rows.stream().filter(r -> "RUNNING".equals(r.statusCode)).findFirst().orElse(null);
		}
		if (picked == null && !rows.isEmpty()) {
			picked = rows.get(rows.size() - 1);
		}

		Map<String, Object> out = new LinkedHashMap<>();
		out.put("machine", machine);
		out.put("pdir", picked);
		out.put("batch", (picked == null) ? null : this.sypMonitorService.currentBatch(picked.pdirNo));

		// PLC 접점 — 명칭의 "N호기" 로 잠정 연결한다. 확정 매핑이 아니다.
		Map<String, Object> plc = new LinkedHashMap<>();
		Integer no = (machine == null) ? null : machine.machineNo;
		plc.put("machineNo", no);
		plc.put("mappingConfirmed", false);
		plc.put("mappingBasis", "PLC 접점 명칭(CD_NM)의 \"N호기\" 문자열. 확정 매핑 아님");
		plc.put("points", (no == null)
				? List.of()
				: this.plcMonitorService.pointsForMachineNo(no, lookback(lookbackHours)));
		out.put("plc", plc);

		// 셋팅온도 — MES 기준정보(PD1016M 배선 + PD1016D 품목별 기준)에서 온다.
		// ★ PLC 가 아니다. 설정값 레지스터는 수집 대상이 아니라서 사람이 입력한 값을 읽는다.
		//    같은 블록의 현재온도는 PLC 실시간값이고, 둘은 출처가 완전히 다르다.
		//
		// 품목은 화면이 이미 고른 지시(지정 > 진행중 > 마지막)의 ITEM_CD 를 쓴다.
		// 계약 §6 "한 설비에 미완료 지시가 2건 이상일 때 어느 것인가" 를 화면 쪽 선택 규칙으로
		// 해소한 것이다. 지시가 없으면 null 이고, 그러면 기준은 기본값(ITEM_CD='')으로 떨어진다.
		String itemCd = (picked == null) ? null : picked.itemCd;

		List<SypZoneTemp> zones = this.sypMonitorService.setTemps(equiCd, itemCd);

		Map<String, Object> setTemp = new LinkedHashMap<>();
		setTemp.put("itemCd", itemCd);
		// 믹서는 ZONE_NO=0 한 행뿐이고 비교할 PLC 접점이 없다(셋팅온도만 표시).
		setTemp.put("mixer", zones.stream()
				.filter(z -> Integer.valueOf(0).equals(z.zoneNo))
				.findFirst().orElse(null));
		// 압출 구역. 구역 축은 PD1016M 이 정식이다 — 접점 순번으로 유추하지 않는다.
		setTemp.put("zones", zones.stream()
				.filter(z -> z.zoneNo != null && z.zoneNo > 0)
				.toList());
		out.put("setTemp", setTemp);

		return out;
	}

	/** 지시가 있는 가장 최근 일자 */
	@GetMapping("/latest-date")
	public Map<String, Object> latestDate() {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("latestPdirDt", this.sypMonitorService.latestPdirDate());
		return out;
	}

	/** 수집 DB 접속이 확인되지 않은 상태. 빈 값을 정상으로 오해하지 않도록 503 으로 돌려준다. */
	@ExceptionHandler(IllegalStateException.class)
	public ResponseEntity<Map<String, Object>> handleGuard(IllegalStateException e) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("error", "SYP_DB_UNAVAILABLE");
		body.put("message", e.getMessage());
		body.put("serverTime", LocalDateTime.now());
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
	}

	private static int lookback(Integer requested) {
		return (requested == null || requested <= 0) ? DEFAULT_LOOKBACK_HOURS : requested;
	}

	private static String resolveDate(String pdirDt) {
		if (pdirDt == null || pdirDt.isBlank()) {
			return LocalDate.now().format(DT);
		}
		// 형식이 틀리면 조용히 넘기지 않고 여기서 걸러 400 이 나게 한다.
		return LocalDate.parse(pdirDt.trim(), DT).format(DT);
	}
}
