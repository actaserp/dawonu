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


  public List<Map<String, Object>> getList(LocalDate start, LocalDate  end, String company) {
    MapSqlParameterSource param = new MapSqlParameterSource();
    param.addValue("start", start);
    param.addValue("end", end);
    param.addValue("company", company);

    String sql = """
       select
         h.id,
         s.vechidno ,
         c."Name" as com_name,
         h."JumunDate",
         m."CustomerBarcode",
         s.devdate  ,
         h."DeliveryDate",
         h.contractnm ,
         d."SujuQty",
         s."Qty" as ship_oty,
         m."Name" as mat_name ,
         GREATEST((d."SujuQty" - s."Qty" ), 0) AS "SujuQty3"
         from shipment s
         left join  suju_head h on h.id = s.suju_head_id
         left join suju d on h.id= d."SujuHead_id"
         left join company c on c.id= h."Company_id"
         left join material m on m.id = d."Material_id"
         where 1=1
         AND h."DeliveryDate" BETWEEN :start AND :end
        """;
    if (StringUtils.isEmpty(company)==false)
      sql+="and upper(c.\"Name\") like concat('%%',upper(:company),'%%')";

    sql+= """
        order by h."DeliveryDate"
        """;

//    log.info("수주별납품현황 SQL: {}", sql);
//    log.info("수주별납품현황 데이터: {}", param.getValues());
    return this.sqlRunner.getRows(sql, param);
  }
}
