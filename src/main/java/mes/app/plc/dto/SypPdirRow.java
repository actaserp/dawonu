package mes.app.plc.dto;

import java.math.BigDecimal;

/**
 * 작업지시 현황 표의 한 행. 레이아웃(현장모니터링_레이아웃.xlsx Sheet1) 상단 표에 대응한다.
 *
 * 산식은 MES_SYPOLYTECH 세션과 합의한 계약이다
 * (agent_network/20260909_현장모니터링화면_작업지시설비_데이터계약.md).
 * 원형 프로시저 USP_PDIR_MONITORING 이 있으나 미완성 상태로 방치돼 있어 그대로 쓰지 않는다.
 */
public class SypPdirRow {

	/** 작업지시번호 */
	public String pdirNo;

	/** 지시순번 */
	public Integer pdirSq;

	/** 지시일자 'yyyy-MM-dd' */
	public String pdirDt;

	/** 설비코드(믹서). 호기 식별자다. */
	public String equiCd;

	/** 업체명 — CD1300M.VEND_NM */
	public String vendNm;

	/** 제품명 — PD1000M.ITEM_NM. ★유일하지 않다. 식별은 반드시 itemCd 로 한다. */
	public String itemNm;

	/** 품목코드. 제품 식별의 유일한 키. */
	public String itemCd;

	/**
	 * 규격 — PD1000M.ITEM_SIZE.
	 * 실제로는 색상 문자열이 들어 있다("국방색", "흑색(BK)"). nvarchar 자유입력이라 코드 변환이 없다.
	 * COLOR_CD 가 아니다 — 그쪽은 전 행 공백이다.
	 */
	public String itemSize;

	/** 작업지시량 — PM1100M.PDIR_WT. 계산하지 않고 그대로 쓴다(아래 batchWt 주석 참조). */
	public BigDecimal pdirWt;

	/** 지시Batch — PM1100M.BATCH_QTY */
	public BigDecimal batchQty;

	/**
	 * 배치당 중량 — PM1100M.BATCH_WT.
	 * PDIR_WT = BATCH_QTY × BATCH_WT 가 항상 성립하지는 않는다(8,143건 중 414건 불일치).
	 * 그래서 작업지시량을 이 값으로 역산하면 안 된다.
	 */
	public BigDecimal batchWt;

	/**
	 * 작업Batch — 이 지시에서 지금까지 생산 완료된 배치 수.
	 * PM1200M 을 CORP_CLS='10' AND PROC_CD='1000' AND HALF_YN='N' AND DEL_YN='N' 로 COUNT 한다.
	 * 스칼라 함수 UFN_PDIR_BATCH_QTY 와 같은 값이며(400건 대조 불일치 0),
	 * 행마다 함수를 부르지 않으려고 GROUP BY 집계로 바꿔 조인한다.
	 * 매칭 0건이면 함수는 NULL 을 돌려주지만 여기서는 0 으로 맞춘다.
	 */
	public BigDecimal workBatchQty;

	/** 생산량 = 작업Batch × 배치당 중량 */
	public BigDecimal producedWt;

	/** 진척률(%) = 생산량 / 작업지시량. 작업지시량이 0이면 null. */
	public BigDecimal progressRate;

	/** 작업상태 코드 — DONE / RUNNING / WAITING */
	public String statusCode;

	/** 작업상태 표시명 — 작업완료 / 진행중 / 대기 */
	public String statusName;

	/** 완료여부 원본값 */
	public String comYn;

	/**
	 * 지시구분 코드 — PM1100M.PDIR_CLS.
	 * 코드그룹은 CD1400M CD_CLS='PD3042' 다 (10 정상 / 20 재작업 / 30 샘플 / 40 소진).
	 * ★ 'PM1020'(10 자동 / 20 수작업)이 아니다. 두 그룹이 같은 코드값에 다른 의미를 담고 있어
	 * 잘못 조인해도 에러 없이 그럴듯한 한글 라벨이 나온다.
	 */
	public String pdirCls;

	/** 지시구분 명칭 */
	public String pdirClsNm;

	/** 확정여부 — PM1100M.ALS_YN. 자동화 여부가 아니다. 현재 화면에서는 거르지 않고 그대로 내보낸다. */
	public String alsYn;
}
