package mes.app.sales.service;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SujuDeliveryStatusService {

  @Autowired
  SqlRunner sqlRunner;


  public List<Map<String, Object>> getList(LocalDate start, LocalDate end, String company) {
    MapSqlParameterSource param = new MapSqlParameterSource()
        .addValue("start", start)
        .addValue("end", end)
        .addValue("company", company);

    String sql = """
        WITH suju_qty AS (
          SELECT d."SujuHead_id" AS head_id, d."Material_id" AS mat_id, SUM(d."SujuQty") AS suju_qty
          FROM suju d
          GROUP BY d."SujuHead_id", d."Material_id"
        ),
        ship_qty AS (
          SELECT d."SujuHead_id" AS head_id, d."Material_id" AS mat_id,
                 COALESCE(SUM(s."Qty"), 0) AS shipped_qty, MAX(s."_created") AS last_ship_ts
          FROM suju d
          LEFT JOIN shipment s ON s."SourceDataPk" = d.id
          GROUP BY d."SujuHead_id", d."Material_id"
        )
        SELECT
          h.id,
          '' AS vechidno,
          c."Name" AS com_name,
          h."JumunDate",
          m."CustomerBarcode",
          to_char(sp.last_ship_ts, 'yyyy-mm-dd') AS devdate,
          h."DeliveryDate",
          h."Description" AS contractnm,
          sq.suju_qty AS "SujuQty",
          COALESCE(sp.shipped_qty, 0) AS "ship_oty",
          GREATEST(sq.suju_qty - COALESCE(sp.shipped_qty, 0), 0) AS "SujuQty3",
          m."Name" AS mat_name
        FROM suju_head h
        JOIN suju_qty sq ON sq.head_id = h.id
        LEFT JOIN ship_qty sp ON sp.head_id = sq.head_id AND sp.mat_id = sq.mat_id
        LEFT JOIN company c ON c.id = h."Company_id"
        LEFT JOIN material m ON m.id = sq.mat_id
        WHERE h."DeliveryDate" BETWEEN :start AND :end
          AND (:company IS NULL OR :company = '' OR UPPER(c."Name") LIKE CONCAT('%%', UPPER(:company), '%%'))
        ORDER BY h."DeliveryDate"
        """;

//    log.info("수주별납품현황 SQL: {}", sql);
//    log.info("수주별납품현황 데이터: {}", param.getValues());
    return this.sqlRunner.getRows(sql, param);
  }

}
