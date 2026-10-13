package mes.app.plc.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import mes.app.plc.service.PlcFreshness;
import mes.app.plc.service.PlcKind;

/**
 * 접점 하나의 메타 + 최신값 + 신선도.
 *
 * 기존 프로시저 22개는 WinForms 라벨에 꽂으려고 AREA_1_TP_VALUE | AREA_2_TP_VALUE 처럼
 * 가로로 펼쳐서 내보냈고, 그래서 화면마다 프로시저가 하나씩 필요했다.
 * 여기서는 접점을 세로로 내보낸다. 화면이 컬럼명에 묶이지 않는다.
 *
 * 단위는 숫자와 분리한다. 기존 USP_MONITORING_VIEW_EQUI 처럼
 * CONCAT(수량,'Kg') 로 붙여 내보내면 받는 쪽이 다시 파싱해야 한다.
 */
public class PlcPoint {

	/** GATH_ID 7자리 = 라인코드(4) + 순번(3) */
	public String id;

	/** 라인코드 4자리. DG2000M 에서는 EQIP_ID 컬럼에 들어 있다("설비ID"가 아니다). */
	public String line;

	/** 수집기 그룹코드 '10' / '20'. DG2000M 에서는 GATH_DESC 컬럼이다("설명"이 아니다). */
	public String collectorGroup;

	/** 레지스터 순번(DSP_SEQ). 그룹별로 1부터 다시 시작한다. */
	public Integer seq;

	/** 접점 명칭(CD_NM). kind 도출의 유일한 근거다. */
	public String name;

	/** CD_NM 에서 도출한 물리량 */
	public PlcKind kind;

	/** 물리 단위. 모르면 null 이고, 추측해서 채우지 않는다. */
	public String unit;

	/** 최신값(GATH_VAL). 조회 구간 안에 수집된 값이 없으면 null. */
	public BigDecimal value;

	/** 변환 전 원문(GATH_DATA) */
	public String raw;

	/** 임계 하한(DG2010M.MIN_VAL) */
	public BigDecimal min;

	/** 임계 상한(DG2010M.MAX_VAL) */
	public BigDecimal max;

	/** 값이 임계범위를 벗어났는지. 값이나 임계값이 없으면 null. */
	public Boolean outOfRange;

	/** 수집시각(DATA_REGDT). smalldatetime 이라 분 해상도이고 초가 없다. */
	public LocalDateTime collectedAt;

	/** 경과 초. 0 미만으로는 내려가지 않는다. */
	public Long ageSeconds;

	public PlcFreshness freshness;

	/** 수집 주기 P(초). CD_V7 크론에서 산출한 값이며 하드코딩이 아니다. */
	public Long cyclePeriodSeconds;
}
