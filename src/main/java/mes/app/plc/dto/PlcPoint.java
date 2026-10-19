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

	/*
	 * ★ 아래 셋은 2026-10-19 부터 항상 null 이다. 채우던 코드를 뺐다.
	 *
	 * 쓰던 곳이 없다 — 현장 화면은 접점 메타·임계값을 표시하지 않고,
	 * 유일한 소비자였던 접점 진단 화면(points.html)도 삭제됐다.
	 * 반면 적재원인 DG2010M 은 489만 행 PK 없는 힙이라 조회가 6초였다.
	 *
	 * 또한 DG2010M 은 임계값 마스터가 아니라 '데이터알림발생현황'(알림 이력)이고,
	 * 2026-10-19 접점 재정의로 옛 접점 기준 임계값이 그대로 남아 의미가 어긋나 있다
	 * (예: 1040013 은 이제 "4호기 전류 13구역"인데 임계는 옛 온도값 141~145).
	 * 임계값이 다시 필요해지면 DG2010M 이 아니라 EM2010M(적용일자·호기·접점·상하한)을 본다.
	 *
	 * 필드 자체는 응답 스키마가 바뀌지 않도록 남겨 둔다(PLC 세션과 합의).
	 */

	/** 임계 하한. 현재 항상 null. */
	public BigDecimal min;

	/** 임계 상한. 현재 항상 null. */
	public BigDecimal max;

	/** 값이 임계범위를 벗어났는지. 현재 항상 null. */
	public Boolean outOfRange;

	/** 수집시각(DATA_REGDT). smalldatetime 이라 분 해상도이고 초가 없다. */
	public LocalDateTime collectedAt;

	/** 경과 초. 0 미만으로는 내려가지 않는다. */
	public Long ageSeconds;

	public PlcFreshness freshness;

	/** 수집 주기 P(초). CD_V7 크론에서 산출한 값이며 하드코딩이 아니다. */
	public Long cyclePeriodSeconds;
}
