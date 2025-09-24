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



	public List<Map<String, Object>> getShipmentList (Integer shipment_header_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("shipment_header_id", shipment_header_id);

		String sql = """
	       select
	        s."ShipmentHead_id" as sh_id
	        , s.id as shipment_id
	        , sh."State"
	        , s."Material_id"
	        , mg."Name" as mat_grp_name
	        , m."Name" as mat_name
	        , m."Code" as mat_code
	        , s."UnitPrice" as unit_price
	        , s."Price" as price
	        , s."Vat" as vat
	        , (s."Price" + s."Vat") as total_price
	        , m."VatExemptionYN" as vat_ex_yn
	        , u."Name" as unit_name 
	        , s."OrderQty"
	        , s."Qty"
	        , s."Description" as description
	        , m."Standard1" as stan_dard --규격
	        , (select coalesce(sum(mlc."OutputQty" ), 0) as lot_qty from mat_lot_cons mlc where mlc."SourceDataPk" = s.id and mlc."SourceTableName"='shipment') as lot_qty
	        from shipment s 
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id" 
	            inner join material m on m.id = s."Material_id" 
	            left join mat_grp mg on mg.id = m."MaterialGroup_id" 
	            left join unit u on u.id = m."Unit_id" 
	        where sh.id = :shipment_header_id	
		        		 """;

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}
}
