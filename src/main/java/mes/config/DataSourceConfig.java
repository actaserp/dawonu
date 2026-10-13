package mes.config;

import java.util.HashMap;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


//import com.zaxxer.hikari.HikariConfig;
//import com.zaxxer.hikari.HikariDataSource;
@EnableJpaRepositories(basePackages = "mes.domain.repository",  entityManagerFactoryRef = "entityManagerFactory", transactionManagerRef = "transactionManager")
@Configuration
public class DataSourceConfig {

	
	@Bean("jdbcTemplate")
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

	// SqlRunner(SqlRunQueryImpl)가 주입받는 NamedParameterJdbcTemplate.
	// 여태 스프링 부트 자동설정이 주 DataSource(PostgreSQL)로 만들어 줬지만,
	// PlcDataSourceConfig 의 plcJdbcTemplate(MS SQL Server) 빈이 생기면서
	// NamedParameterJdbcTemplateAutoConfiguration 의 @ConditionalOnMissingBean 이
	// 물러나 버렸다. 그러면 컨텍스트에 PLC(SQL Server)용 하나만 남아
	// 다원유 MES 쿼리가 전부 SY폴리텍 SQL Server 로 날아간다.
	// 다원유 PostgreSQL 용을 명시적으로 @Primary 로 만들어 둔다.
	@Bean(name = "namedParameterJdbcTemplate")
	@Primary
	NamedParameterJdbcTemplate namedParameterJdbcTemplate(@Qualifier("dataSource") DataSource dataSource) {
		return new NamedParameterJdbcTemplate(dataSource);
	}
	
	// PLC용 두 번째 DataSource가 추가되어 DataSource 후보가 2개가 된다.
	// @Primary가 없으면 JdbcTemplateAutoConfiguration(@ConditionalOnSingleCandidate)이
	// 물러나면서 NamedParameterJdbcTemplate 빈이 사라지고 SqlRunQueryImpl 주입이 깨진다.
	@Primary
	@ConfigurationProperties(prefix="spring.datasource.hikari")
	@Bean(name="dataSource")	
	DataSource dataSource() {
		/*
		HikariConfig config = new HikariConfig();		
		config.setJdbcUrl( "jdbc:postgresql://10.10.10.231:5432/mes_java" );
        config.setUsername( "mes21" );
        config.setPassword( "mes7033" );
        config.addDataSourceProperty( "cachePrepStmts" , true );
        config.addDataSourceProperty( "prepStmtCacheSize" , 250 );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , 2048 );		
		HikariDataSource ds = new HikariDataSource(config);
		return ds;
		*/
		//DRIVER=Devart ODBC Driver for PostgreSQL;Data Source=localhost;Database=mes_db;User ID=actasmes;Password=actas5200
		/*
		HikariConfig config = new HikariConfig();		
		config.setJdbcUrl( "jdbc:postgresql://localhost:5432/mes_db" );
        config.setUsername( "actasmes" );
        config.setPassword( "actas5200" );
        config.addDataSourceProperty( "cachePrepStmts" , true );
        config.addDataSourceProperty( "prepStmtCacheSize" , 250 );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , 2048 );		
		HikariDataSource ds = new HikariDataSource(config);
		return ds;
		 */
		return DataSourceBuilder.create().build();
	}	
	
	@Bean(name="entityManagerFactory")
	LocalContainerEntityManagerFactoryBean entityManagerFactory(){
		LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
		emf.setDataSource(this.dataSource());
		emf.setPackagesToScan(new String[]{"mes.domain.entity"});
		HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
		emf.setJpaVendorAdapter(vendorAdapter);
		HashMap<String, Object> properties =new HashMap<>();
		properties.put("hibernate.ddl-auto", "validate");
		properties.put("hibernate.format_sql",true);
		properties.put("hibernate.show-sql",true);
		properties.put("hibernate.dialect","org.hibernate.dialect.PostgreSQLDialect");
		//properties.put("hibernate.storage_engine", property.getStorage_engine());
		emf.setJpaPropertyMap(properties);	
		return emf;
    } 
	
	@Bean
	@Primary
	PlatformTransactionManager transactionManagerCustom(){
		
		// MyBatis transactional
		//DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager();
		//dataSourceTransactionManager.setDataSource(dataSource());
		
		// JPA transactional
		//JpaTransactionManager jpaTransactionManager = new JpaTransactionManager();
		//jpaTransactionManager.setEntityManagerFactory(entityManagerFactory().getObject());
		
		// Chained transaction manager (MyBatis X JPA)
		//ChainedTransactionManager transactionManager = new ChainedTransactionManager(jpaTransactionManager, dataSourceTransactionManager);
		//return transactionManager;
		
		JpaTransactionManager transactionManager =new JpaTransactionManager();
		transactionManager.setEntityManagerFactory(this.entityManagerFactory().getObject()); 
		return transactionManager;	
	}
	
	@Bean  
	TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {  
	    return new TransactionTemplate(transactionManager);  
	}	
	
	
	@Bean
	SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception{
		final SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
		sessionFactory.setDataSource(dataSource);
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		sessionFactory.setMapperLocations(resolver.getResources("mapper/*.xml")); 	//mapper 파일 로드
		//sessionFactory.setConfigLocation(resolver.getResource("mybatis-config.xml"));//mybatis-config 로드
		return sessionFactory.getObject();
	}
	
	@Bean
	SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory sqlSessionFactory) throws Exception{
		final SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
		return sqlSessionTemplate;
	}


}
