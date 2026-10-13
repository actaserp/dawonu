package mes.config;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.context.SecurityContextRepository;

/**
 * SY폴리텍 PLC 모니터링 전용 보안 체인.
 *
 * 다원유 MES 로그인과 완전히 분리된 별도 realm 이다.
 *
 * 분리가 성립하는 이유는 세션 저장 키를 따로 쓰기 때문이다.
 * 두 체인이 기본값인 SPRING_SECURITY_CONTEXT 를 공유하면, PLC 로그인 한 번으로
 * MES 체인의 anyRequest().authenticated() 까지 통과해 MES 전체가 열린다
 * (MES 체인은 principal 종류를 따지지 않는다).
 * 그래서 PLC 체인은 PLC_SECURITY_CONTEXT 키를 쓴다. 서로의 로그인이 보이지 않는다.
 *
 * 인증 매니저도 부모 없이 직접 지정한다. 부모가 붙으면 PLC 인증 실패가
 * MES 의 CustomAuthenticationManager 로 넘어가 MES 계정으로 PLC 화면에 들어올 수 있다.
 */
@Configuration
public class PlcSecurityConfig {

	/** MES 세션 컨텍스트와 겹치지 않게 하는 전용 키. 이게 분리의 핵심이다. */
	static final String PLC_CONTEXT_KEY = "PLC_SECURITY_CONTEXT";

	/** PLC 모니터링 열람 권한. 이 권한이 있어야 /plc/** 와 /api/plc/** 를 볼 수 있다. */
	static final String PLC_AUTHORITY = "PLC_MONITOR";

	@Value("${plc.auth.username}")
	private String plcUsername;

	@Value("${plc.auth.password}")
	private String plcPassword;

	@Bean
	@Order(1)
	SecurityFilterChain plcFilterChain(HttpSecurity http) throws Exception {

		SecurityContextRepository plcContextRepository = plcSecurityContextRepository();

		http
			// 이 체인은 PLC 경로만 맡는다. 나머지는 기존 MES 체인이 그대로 처리한다.
			.requestMatchers(m -> m.antMatchers("/plc/**", "/api/plc/**", "/api/syp/**"))

			.authorizeHttpRequests(auth -> auth
					.antMatchers("/plc/login", "/plc/login-process").permitAll()
					.anyRequest().hasAuthority(PLC_AUTHORITY))

			// 부모 없는 단독 ProviderManager. MES 계정으로는 절대 통과할 수 없다.
			.authenticationManager(new ProviderManager(List.of(plcAuthenticationProvider())))

			.securityContext(ctx -> ctx.securityContextRepository(plcContextRepository))

			.formLogin(form -> form
					.loginPage("/plc/login")
					.loginProcessingUrl("/plc/login-process")
					.defaultSuccessUrl("/plc/monitor", true)
					.failureUrl("/plc/login?error")
					.permitAll())

			.logout(out -> out
					.logoutUrl("/plc/logout")
					.logoutSuccessUrl("/plc/login?logout")
					// 세션을 통째로 버리면 같은 브라우저에 열려 있던 MES 세션까지 끊긴다.
					// 인증만 지우면 위 저장소가 PLC_SECURITY_CONTEXT 속성만 제거한다.
					.invalidateHttpSession(false)
					.clearAuthentication(true)
					.permitAll())

			.exceptionHandling(ex -> ex
					.authenticationEntryPoint(new LoginUrlAuthenticationEntryPoint("/plc/login")));

		return http.build();
	}

	private SecurityContextRepository plcSecurityContextRepository() {
		HttpSessionSecurityContextRepository repo = new HttpSessionSecurityContextRepository();
		repo.setSpringSecurityContextKey(PLC_CONTEXT_KEY);
		return repo;
	}

	/**
	 * server.env 의 SYP_PLC_LOGIN_ID / SYP_PLC_LOGIN_PW 하나만 검증한다.
	 * 빈으로 등록하지 않는다 - 전역 AuthenticationManagerBuilder 에 잡히면
	 * 기존 MES 인증 구성까지 건드리게 된다.
	 */
	private AuthenticationProvider plcAuthenticationProvider() {
		return new AuthenticationProvider() {
			@Override
			public Authentication authenticate(Authentication authentication) throws AuthenticationException {
				String id = String.valueOf(authentication.getName());
				String pw = String.valueOf(authentication.getCredentials());

				// 길이 노출을 줄이려고 상수시간 비교를 쓴다.
				boolean ok = constantTimeEquals(plcUsername, id) & constantTimeEquals(plcPassword, pw);
				if (!ok) {
					throw new BadCredentialsException("PLC 모니터링 계정이 올바르지 않습니다.");
				}
				return new UsernamePasswordAuthenticationToken(
						id, null, List.of(new SimpleGrantedAuthority(PLC_AUTHORITY)));
			}

			@Override
			public boolean supports(Class<?> authentication) {
				return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
			}
		};
	}

	private static boolean constantTimeEquals(String expected, String actual) {
		if (expected == null || actual == null) {
			return false;
		}
		byte[] a = expected.getBytes(java.nio.charset.StandardCharsets.UTF_8);
		byte[] b = actual.getBytes(java.nio.charset.StandardCharsets.UTF_8);
		return java.security.MessageDigest.isEqual(a, b);
	}
}
