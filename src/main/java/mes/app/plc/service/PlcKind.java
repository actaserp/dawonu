package mes.app.plc.service;

/**
 * 접점이 재는 물리량.
 *
 * 판정 근거는 CD1400M(CD_CLS='DG1010').CD_NM 접점 명칭 하나뿐이다.
 * DB 컬럼명으로 분류하면 안 된다. 기존 프로시저 USP_MONITORING_VIEW7_EC 는
 * 컬럼을 AREA_n_EC_VALUE(EC=전류)로 내보내면서 실제로는 RPM 을 담고 있어,
 * 컬럼명 기준으로 분류하면 1054001~1054004 가 전류로 잘못 나간다.
 *
 * 판정 순서와 UNKNOWN 처리는 PLC 세션과의 공동 계약이라 임의로 바꾸지 않는다.
 */
public enum PlcKind {

	RPM("rpm"),
	TEMP("C"),
	CURRENT("A"),
	PRESSURE(null),   // 단위 미확정. 확인 전까지 추측해서 채우지 않는다.
	POWER(null),      // 전원 접점. 물리 단위 없음
	UNKNOWN(null);

	private final String unit;

	PlcKind(String unit) {
		this.unit = unit;
	}

	public String getUnit() {
		return this.unit;
	}

	/**
	 * 접점 명칭에서 물리량을 도출한다.
	 *
	 * 판정 순서는 계약대로 RPM → TEMP → CURRENT → PRESSURE → POWER 이고,
	 * 어느 것에도 걸리지 않으면 UNKNOWN 이다. 추측해서 채우지 않는다.
	 *
	 * 순서가 결과를 바꾸는 실제 사례가 있다.
	 * 1060001 "1호기 교반기 압력온도" 는 압력과 온도를 모두 포함하는데,
	 * TEMP 가 PRESSURE 보다 먼저라 TEMP 로 판정된다.
	 */
	public static PlcKind fromName(String cdNm) {
		if (cdNm == null) {
			return UNKNOWN;
		}
		String n = cdNm.toUpperCase();

		if (n.contains("RPM")) {
			return RPM;
		}
		if (n.contains("온도")) {
			return TEMP;
		}
		if (n.contains("전류")) {
			return CURRENT;
		}
		if (n.contains("압력")) {
			return PRESSURE;
		}
		if (n.contains("전원")) {
			return POWER;
		}
		return UNKNOWN;
	}
}
