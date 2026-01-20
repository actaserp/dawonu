package mes.domain.repository;

import mes.domain.entity.Bom;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

@Repository
public class BomRuleRepository {

    @Autowired
    private NamedParameterJdbcTemplate jdbc;

    /**
     * materialCode 기준
     * user_code(Parent_id=330) 중
     * 가장 긴 prefix 매칭 코드
     */
    public String findLongestRule(String materialCode) {

        String sql = """
            SELECT B."Code"
            FROM user_code B
            WHERE B."Parent_id" = 330
              AND :materialCode LIKE B."Code" || '%'
            ORDER BY LENGTH(B."Code") DESC
            LIMIT 1
        """;

        MapSqlParameterSource param = new MapSqlParameterSource()
                .addValue("materialCode", materialCode);

        List<String> result = jdbc.query(
                sql,
                param,
                (rs, rowNum) -> rs.getString("Code")
        );

        return result.isEmpty() ? null : result.get(0);
    }

    /**
     * 조합 정의 코드(ruleCode)를
     * 실제 BOM 단품으로 분해
     *
     * 예)
     * ZL250TR135 → ZL250, TR1
     */
    public List<String> findLeafMaterials(String ruleCode) {

        String sql = """
            SELECT C."Code"
            FROM user_code C
            WHERE C."Parent_id" = (
                SELECT id
                FROM user_code
                WHERE "Code" = :ruleCode
            )
            ORDER BY C."Code"
        """;

        MapSqlParameterSource param = new MapSqlParameterSource()
                .addValue("ruleCode", ruleCode);

        return jdbc.query(
                sql,
                param,
                (rs, rowNum) -> rs.getString("Code")
        );
    }
}
