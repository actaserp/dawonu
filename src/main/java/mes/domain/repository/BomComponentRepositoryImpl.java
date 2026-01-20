package mes.domain.repository;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class BomComponentRepositoryImpl
        implements BomComponentRepositoryCustom {

    private final NamedParameterJdbcTemplate jdbc;

    public BomComponentRepositoryImpl(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void insertIfNotExists(Long bomId,
                                  Integer materialId,
                                  int amount) {

        String sql = """
        INSERT INTO bom_comp (
            "_created",
            "BOM_id",
            "Material_id",
            "Amount",
            "spjangcd"
        )
        SELECT
            now(),
            :bomId,
            :materialId,
            :amount,
            'ZZ'
        WHERE NOT EXISTS (
            SELECT 1
            FROM bom_comp
            WHERE "BOM_id" = :bomId
              AND "Material_id" = :materialId
        )
    """;

        MapSqlParameterSource param = new MapSqlParameterSource()
                .addValue("bomId", bomId)
                .addValue("materialId", materialId)
                .addValue("amount", amount);

        jdbc.update(sql, param);
    }

}
