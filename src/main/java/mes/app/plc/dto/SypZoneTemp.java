package mes.app.plc.dto;

import java.math.BigDecimal;

/**
 * 셋팅온도 한 칸. PD1016M(배선) + PD1016D(품목별 기준)에서 나온다.
 *
 * 계약: agent_network/20261020_대시보드_셋팅온도_품목별기준_데이터계약.md (2차 개정)
 *
 * ★ 셋팅온도는 PLC 접점이 아니다. 수집기는 계측값 레지스터만 읽고 설정값은 수집 대상이 아니라서,
 *   MES 기준정보화면(PD1016A10)에 사람이 입력한 값을 읽어 온다.
 *   반대로 현재온도는 PLC 실시간값이다 — 둘은 출처가 완전히 다르다.
 *
 * 압출기는 구역(ZONE_NO 1..n)별로 한 행, 믹서는 구역이 없어 ZONE_NO=0 한 행뿐이고
 * 믹서는 GATH_ID 가 빈 문자열이다(비교할 PLC 접점이 없다).
 */
public class SypZoneTemp {

	/** 공정. '1000' 믹서 / '2000' 압출·포장 */
	public String procCd;

	/** 설비코드. 믹서와 압출이 서로 다른 값이다(LANE_CD 로 짝지은다) */
	public String equiCd;

	public String equiNm;

	/** 압출 = 1..n, 믹서 = 0 */
	public Integer zoneNo;

	public String zoneNm;

	/**
	 * 이 구역에 배선된 PLC 온도접점ID. 현재온도와 맞출 때 쓰는 유일한 키다.
	 * 믹서는 null(원래 값은 빈 문자열) — 접점이 없다.
	 *
	 * ★ 구역 순번으로 접점을 유추하지 마라. PD1016M 이 정식 배선이다.
	 */
	public String gathId;

	/** 셋팅온도. null 이면 "기준없음"이고 0℃ 와 다르다(결함 F4). */
	public BigDecimal setTemp;

	/**
	 * 기준 출처. 조회 우선순위 중 어느 단계에서 나온 값인지.
	 * ITEM = 그 품목 전용 / DEFAULT = ITEM_CD='' 기본값 / NONE = 둘 다 없음
	 */
	public String source;
}
