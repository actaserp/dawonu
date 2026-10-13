package mes.config;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * SY폴리텍 PLC 수집 DB(MS SQL Server) 전용 DataSource.
 *
 * 이 프로젝트의 주 DataSource는 PostgreSQL(다원유 HACCP MES)이고 그건 그대로 둔다.
 * PLC 모니터링만 이 DataSource를 쓰며, 조회 전용이다.
 * 적재는 수집기 ActasPLC.exe / USP_DG110120L10_A10 전담이므로 여기서 쓰기는 하지 않는다.
 */
@Configuration
public class PlcDataSourceConfig {

	private static final Logger log = LoggerFactory.getLogger(PlcDataSourceConfig.class);

	/**
	 * 접속이 허용된 DB 목록.
	 *
	 * 이 화면은 SELECT 전용 조회다(쓰기 엔드포인트 없음 + 커넥션 read-only). 그래서 운영
	 * FactMES_SYP 를 직접 읽어도 쓰기 사고가 나지 않는다. 운영 대시보드는 당연히 운영 DB를 본다.
	 *
	 * 다만 서버 주소가 같고 이름이 한 글자(_TEST) 차이라, '전혀 엉뚱한 DB에 붙는' 설정 사고는
	 * 여전히 막는다. 설정 문자열을 신뢰하지 않고 실제 접속 후 DB_NAME() 으로 다시 확인해서,
	 * 아래 목록에 없는 DB면 PlcGuard 를 잠그고 모든 조회를 거부한다.
	 *
	 * 목록은 plc.datasource.allowed-databases 로 바꿀 수 있다(쉼표 구분). 기본값은 운영+TEST.
	 */
	@Value("${plc.datasource.allowed-databases:FactMES_SYP,FactMES_SYP_TEST}")
	private String allowedDatabasesCsv;

	@Bean(name = "plcDataSource")
	@ConfigurationProperties(prefix = "plc.datasource")
	DataSource plcDataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean(name = "plcJdbcTemplate")
	NamedParameterJdbcTemplate plcJdbcTemplate(@Qualifier("plcDataSource") DataSource plcDataSource) {
		return new NamedParameterJdbcTemplate(plcDataSource);
	}

	/**
	 * 기동 시 접속 대상 DB를 검증한다.
	 *
	 * 검증에 실패해도 애플리케이션은 계속 뜬다. PLC 모니터링은 다원유 MES에 얹은 부가 기능이라
	 * 여기서 예외를 던져 MES 본체까지 못 뜨게 만드는 쪽이 더 나쁘다.
	 * 대신 PlcGuard 가 잠기고 모든 PLC 조회가 거부된다.
	 */
	@Bean
	PlcGuard plcGuard(@Qualifier("plcDataSource") DataSource plcDataSource) {
		Set<String> allowed = Arrays.stream(allowedDatabasesCsv.split(","))
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(Collectors.toCollection(LinkedHashSet::new));

		PlcGuard guard = new PlcGuard(allowed);
		try (var cn = plcDataSource.getConnection();
			 var st = cn.createStatement();
			 var rs = st.executeQuery("SELECT DB_NAME()")) {
			String db = rs.next() ? rs.getString(1) : null;
			if (db != null && allowed.contains(db)) {
				guard.unlock(db);
				log.info("[PLC] 수집 DB 접속 확인: {} (허용 목록: {})", db, allowed);
			} else {
				log.error("[PLC] 접속 대상 DB가 허용 목록 {} 에 없다(실제: {}). PLC 조회를 전부 차단한다.", allowed, db);
			}
		} catch (Exception e) {
			log.error("[PLC] 수집 DB 접속 실패. PLC 조회를 전부 차단한다. ({})", e.getMessage());
		}
		return guard;
	}

	/**
	 * 검증된 DB에 붙었을 때만 열리는 잠금장치. 모든 PLC 조회가 이걸 먼저 통과해야 한다.
	 */
	public static class PlcGuard {
		private final Set<String> allowed;
		private volatile boolean open = false;
		private volatile String database = null;

		PlcGuard(Set<String> allowed) {
			this.allowed = allowed;
		}

		void unlock(String db) {
			this.database = db;
			this.open = true;
		}

		public boolean isOpen() {
			return this.open;
		}

		public String getDatabase() {
			return this.database;
		}

		public void check() {
			if (!this.open) {
				throw new IllegalStateException(
						"PLC 수집 DB(" + this.allowed + ") 접속이 확인되지 않아 조회를 거부한다.");
			}
		}
	}
}
