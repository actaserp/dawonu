package mes.app.plc.service;

/**
 * 값의 신선도. P 는 수집 주기(초)이며 CD1400M(CD_CLS='DG0900').CD_V7 크론에서 산출한다.
 *
 *   FRESH  age ≤ P × 3
 *   STALE  그 사이
 *   DEAD   age > max(P × 10, 30분)
 *
 * 기존 대시보드는 조회에 신선도 조건도 CORP_CD 조건도 없어서(F7),
 * 수집이 9개월 멈춰 있어도 옛날 값을 정상값처럼 보여줬다. 그래서 아무도 몰랐다.
 */
public enum PlcFreshness {

	FRESH,
	STALE,
	DEAD;

	/** STALE 판정의 하한(초). 수집 주기가 아주 짧아도 5분 전에는 STALE 로 몰지 않는다. */
	private static final long STALE_FLOOR_SECONDS = 5 * 60;

	/** DEAD 판정의 하한(초). 수집 주기가 아주 짧아도 30분 전에는 DEAD 로 몰지 않는다. */
	private static final long DEAD_FLOOR_SECONDS = 30 * 60;

	/**
	 * @param ageSeconds 수집시각으로부터 경과한 초. 음수는 호출 전에 0으로 클램프되어 있어야 한다.
	 * @param periodSeconds 수집 주기 P
	 */
	public static PlcFreshness of(Long ageSeconds, Long periodSeconds) {
		// 조회 구간 안에 값이 아예 없으면 수집이 끊긴 것이다.
		if (ageSeconds == null) {
			return DEAD;
		}
		// 주기를 모르면 신선하다고 말할 근거가 없다.
		if (periodSeconds == null || periodSeconds <= 0) {
			return DEAD;
		}

		long staleLimit = Math.max(periodSeconds * 10, STALE_FLOOR_SECONDS);
		long deadLimit = Math.max(staleLimit, DEAD_FLOOR_SECONDS);

		if (ageSeconds <= periodSeconds * 3) {
			return FRESH;
		}
		if (ageSeconds <= deadLimit) {
			return STALE;
		}
		return DEAD;
	}
}
