package mes.app.plc.dto;

/**
 * 호기 한 대. 레이아웃 좌상단 "1호기" 와 우상단 "설비상태" 에 대응한다.
 *
 * 호기 = EM1000M 중 PROC_CD='1000'(믹서) 설비다.
 * 원형 프로시저는 LANE_CD 를 쓰지 않으므로 EQUI_CD 를 키로 쓰고 LANE_CD 는 참조만 한다.
 */
public class SypMachine {

	/** 설비코드(믹서). 호기 식별자. */
	public String equiCd;

	/** 설비명 — "믹서1호기", "믹서7호기(XLPE)" 등 */
	public String equiNm;

	/**
	 * 라인코드 S1~S12. 활성 믹서 12건과 1:1이다.
	 * ★ PLC 접점의 라인코드(1010, 1020 …)와는 다른 체계이며 매핑이 아직 없다.
	 */
	public String laneCd;

	/**
	 * 화면 정렬 순서.
	 * ★ EQUI_CD 순 ≠ 호기 번호순이다 (6호기=10000008, 7호기=10000006, 10호기=10000025).
	 * 반드시 이 값으로 정렬해야 호기가 순서대로 나온다.
	 */
	public Integer orderNum;

	/**
	 * 설비명에서 뽑은 호기 번호("믹서7호기(XLPE)" -> 7).
	 * PLC 접점명의 "N호기"와 잇는 잠정 연결키다. MachineNo 주석 참조.
	 */
	public Integer machineNo;

	/**
	 * 설비상태 — 가동중 / 수리 / 정지.
	 *
	 * ★ 현재 판정하지 않고 항상 null 이다. 사용자 결정 대기 중이다.
	 *
	 * MES 스키마에 설비 가동상태 컬럼이 없다(EM1000M 은 USE_YN 뿐).
	 * '수리'만 EM1100M 미완료 고장건으로 근거가 있고, '가동중/정지'는 근거가 없다.
	 * 작업지시 기반으로 판정하면 PLC 인수인계서 §8의 결함 F8을 그대로 재생산한다 —
	 * 기존 USP_MONITORING_VIEW_EQUI 가 작업지시 수량으로 판정해서
	 * 지시만 걸려 있으면 설비가 꺼져 있어도 '동작중'으로 뜬다.
	 * 없는 근거로 '가동중'을 찍느니 비워 둔다.
	 */
	public String statusName;

	/** 이 호기의 해당 일자 지시 건수 요약 */
	public int totalCount;
	public int doneCount;
	public int runningCount;
	public int waitingCount;
}
