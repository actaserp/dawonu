package mes.domain.repository;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class BomRepositoryImpl implements BomRepositoryCustom {

    private final NamedParameterJdbcTemplate jdbc;

    public BomRepositoryImpl(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Long ensureBom(Integer materialId, boolean dryRun) {

        if (!dryRun) {
            String insertSql = """
            INSERT INTO bom (
                   "_created",
                   "Material_id",
                   "Name",
                   "BOMType",
                   "OutputAmount",
                   "spjangcd",
                   "StartDate",
                   "EndDate"
               )
               SELECT
                   now(),                               -- 생성 시각
                   m.id,
                   m."Name",
                   'manufacturing',
                   1,
                   'ZZ',
                   TIMESTAMPTZ '2026-01-01 00:00:00+09',                               -- StartDate
                   TIMESTAMPTZ '2100-12-31 23:59:59+09'  -- EndDate
               FROM material m
               WHERE m.id = :materialId
                 AND NOT EXISTS (
                     SELECT 1
                     FROM bom b
                     WHERE b."Material_id" = m.id
                       AND b."BOMType" = 'manufacturing'
                 )
        """;


            jdbc.update(
                    insertSql,
                    new MapSqlParameterSource("materialId", materialId)
            );
        }

        String selectSql = """
        SELECT id
        FROM bom
        WHERE "Material_id" = :materialId
          AND "BOMType" = 'manufacturing'
        ORDER BY id DESC
        LIMIT 1
    """;

        return jdbc.queryForObject(
                selectSql,
                new MapSqlParameterSource("materialId", materialId),
                Long.class
        );
    }


}
