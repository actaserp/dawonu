package mes.domain.services.impl;


import java.util.List;
import java.util.Map;

import org.hibernate.exception.DataException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import mes.domain.services.LogWriter;
import mes.domain.services.SqlRunner;

import javax.annotation.PostConstruct;


@Repository
public class SqlRunQueryImpl implements SqlRunner {

	@Autowired(required = true)
    private NamedParameterJdbcTemplate  jdbcTemplate;
	
	@Autowired
	LogWriter logWriter;
	
	
    public List<Map<String, Object>> getRows(String sql, MapSqlParameterSource dicParam){    	
    	
    	List<Map<String, Object>> rows = null;
    	
    	
    	try {
    		rows = this.jdbcTemplate.queryForList(sql, dicParam);
		} 
    	catch(DataAccessException de) {
    		System.out.println(de);
    	}
    	catch (Exception e) {
			// TODO: handle exception
			logWriter.addDbLog("error", "SqlRunQueryImpl.getRows", e);
			throw e;
		}
    	return rows;
    }

	public Map<String, Object> getRow(String sql, MapSqlParameterSource dicParam) {
		try {
			List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, dicParam);
			return list.isEmpty() ? null : list.get(0);
		} catch (Exception e) {
			logWriter.addDbLog("error", "SqlRunQueryImpl.getRow", e);
			throw e;
		}
	}

	@PostConstruct
	public void debugLoaded() {
		System.out.println("### SqlRunQueryImpl loaded from: ");
		System.out.println("### -> " + this.getClass().getProtectionDomain().getCodeSource().getLocation());
	}
    
    public int execute(String sql, MapSqlParameterSource dicParam) {
    	
    	int rowEffected = 0;
    	// TODO Auto-generated method stub
    	try {
    		rowEffected = this.jdbcTemplate.update(sql, dicParam);
		} catch (Exception e) {
			// TODO: handle exception
			logWriter.addDbLog("error", "SqlRunQueryImpl.excute", e);
		}
    	
    	return rowEffected;
    }
    
    public int queryForCount(String sql,  MapSqlParameterSource dicParam) {
    	//select count(*) from xxx where ~
    	return this.jdbcTemplate.queryForObject(sql, dicParam, int.class);
    }
    
    public <T> T queryForObject(String sql,  MapSqlParameterSource dicParam, RowMapper<T> mapper) throws DataException {
    	T rr= this.jdbcTemplate.queryForObject(sql, dicParam, mapper); 
    	return rr;    	
    }

	public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs) {
		int[] result = new int[0];

		try {
			result = this.jdbcTemplate.batchUpdate(sql, batchArgs);
		} catch (DataAccessException de) {
			System.out.println(de);
		} catch (Exception e) {
			logWriter.addDbLog("error", "SqlRunQueryImpl.batchUpdate", e);
		}

		return result;
	}

    
}
