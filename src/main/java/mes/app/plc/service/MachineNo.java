package mes.app.plc.service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 이름 문자열에서 "N호기"의 N을 뽑는다.
 *
 * MES 설비명("믹서1호기", "믹서7호기(XLPE)")과 PLC 접점명("1호기 생산라인 온도 1구역")이
 * 같은 표기를 쓰고 있어, 이걸로 두 체계를 잇는다.
 *
 * ★ 이건 정식 매핑이 아니라 명칭에서 도출한 잠정 연결이다.
 * MES 의 호기 키는 EM1000M.EQUI_CD(또는 LANE_CD S1~S12)이고
 * PLC 의 키는 라인코드(1010, 1020 …)인데, 둘을 잇는 마스터가 어디에도 없다.
 * PLC 세션과 확인이 끝나기 전까지 화면에 "잠정"으로 표시한다.
 *
 * 이름 기반 도출이 근본 해법이 아닌 건 kind 도출(§4-3)과 같은 사정이다.
 * 접점 재작성 때 CD1400M 에 호기 코드 컬럼을 두는 것이 정답이다.
 */
public final class MachineNo {

	/** "믹서1호기", "1호기 생산라인 온도 1구역", "압출/포장11호기" 모두 잡는다. */
	private static final Pattern P = Pattern.compile("(\\d{1,2})\\s*호기");

	private MachineNo() {
	}

	/** 못 찾으면 null */
	public static Integer parse(String name) {
		if (name == null) {
			return null;
		}
		Matcher m = P.matcher(name);
		return m.find() ? Integer.valueOf(m.group(1)) : null;
	}
}
