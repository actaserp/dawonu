package mes.app.pda.service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.app.util.UtilClass;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

@Service
public class ShipmentApiService {

    @Autowired
    SqlRunner sqlRunner;


    public List<Map<String, Object>> ApigetShipmentOrderList(String date_from, String date_to, String state, Integer comp_pk, Integer mat_grp_pk, Integer mat_pk, String keyword) {

        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("date_from", Date.valueOf(date_from));
        paramMap.addValue("date_to", Date.valueOf(date_to));
        paramMap.addValue("state", state);
        paramMap.addValue("comp_pk", comp_pk);
        paramMap.addValue("mat_grp_pk", mat_grp_pk);
        paramMap.addValue("mat_pk", mat_pk);
        paramMap.addValue("keyword", "%" + UtilClass.getStringSafe(keyword) + "%");

        String sql = """
				select --sh.id
				  sh.id
				 ,su."JumunNumber" as jumun_number
				, c."Name" as company_name
				--, s."Material_id" as material_id
					
				,(min(m."Name") ||\s
				 case when count(distinct m."Name") > 1\s
				      then ' 외 ' || (count(distinct m."Name") - 1) || '건'\s
				      else '' end
				) as material_name_summary
				, sum(s."OrderQty") ::int as total_qty
				, sum(s."Qty") ::int 		as qty
				, sh."OrderDate" as order_date
				, su."DueDate"  as due_date -- 납기일
				--, sh."ShipDate" as ship_date  --출하일
				, sh."State" as state
                from shipment s 
                
                left outer join shipment_head sh
				on sh.id = s."ShipmentHead_id"   
                
                left outer join suju su
				on su.id = s."SourceDataPk"
				
				inner join material m
				on s."Material_id" = m.id
				
				join company c on c.id = sh."Company_id"
				
				where sh."OrderDate" between :date_from and :date_to
				and c."Name" like :keyword
				""";

				if(StringUtils.isEmpty(state) == false){
					sql += "  and sh.\"State\" = :state ";
				}

				sql += """
						group by sh.id, su."JumunNumber", c."Name", sh."OrderDate", su."DueDate", sh."State"
						order by su."JumunNumber"
						""";




        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
    }
}
