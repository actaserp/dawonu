package mes.app.plc;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

/**
 * SY폴리텍 PLC 모니터링 화면.
 *
 * 다원유 MES 메뉴 체계(gui.json, menu, user_group_menu)에는 넣지 않는다.
 * 다른 회사 설비 데이터이고, 나중에 별도 프로젝트로 떼어내기도 쉬워야 한다.
 * 인증도 다원유 MES 로그인과 분리되어 있다. PlcSecurityConfig 참조.
 */
@Controller
@RequestMapping("/plc")
public class PlcViewController {

	@GetMapping("/login")
	public ModelAndView login(
			@RequestParam(value = "error", required = false) String error,
			@RequestParam(value = "logout", required = false) String logout) {

		ModelAndView mv = new ModelAndView("plc/login");
		mv.addObject("loginFailed", error != null);
		mv.addObject("loggedOut", logout != null);
		return mv;
	}

	/** 현장 모니터링 본화면. 레이아웃(현장모니터링_레이아웃.xlsx Sheet1) 기준. */
	@GetMapping({ "", "/", "/monitor" })
	public ModelAndView monitor() {
		return new ModelAndView("plc/monitor");
	}
}
