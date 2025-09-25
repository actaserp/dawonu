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
		paramMap.addValue("keyword", keyword);

		String sql = """
				with sg as (
				SELECT\s
				    s."ShipmentHead_id",
				    SUM(s."Qty") AS total_qty,
				    MIN(m."Name") ||\s
				      CASE\s
				        WHEN COUNT(DISTINCT m."Name") > 1\s
				        THEN ' 외 ' || (COUNT(DISTINCT m."Name") - 1) || '건'
				        ELSE ''
				      END AS item_names
				FROM shipment s
				JOIN material m ON m.id = s."Material_id"
				GROUP BY s."ShipmentHead_id"
				)
				    
				select sh.id
				, sh."Company_id" as company_id
				, c."Name" as company_name
				, sh."ShipDate" as ship_date
				, sh."TotalQty" as total_qty
				, sh."TotalPrice" as total_price
				, sh."TotalVat" as total_vat
				, sh."Description" as description
				, sh."State" as state
				, fn_code_name('shipment_state', sh."State") as state_name
				, to_char(coalesce(sh."OrderDate",sh."_created") ,'yyyy-mm-dd') as order_date
				, sh."StatementIssuedYN" as issue_yn
				, sh."StatementNumber" as stmt_number\s
				, sh."IssueDate" as issue_date
				, sh."DeliveryName" as delivery_name
				, sg."item_names" as material_name_summary
				from shipment_head sh\s
				join company c on c.id = sh."Company_id"
				join sg on sg."ShipmentHead_id" = sh.id
                where sh."ShipDate"  between :date_from and :date_to
				         """;
		if (comp_pk != null) {
			sql += " and sh.\"Company_id\" = :comp_pk ";
		}

		if (StringUtils.isEmpty(state) == false) {
			sql += "  and sh.\"State\" = :state ";
		}

		if (mat_pk != null || mat_grp_pk != null || StringUtils.isEmpty(keyword) == false) {
			sql += """
					and exists ( select 1
            		    from shipment s 
                        inner join material m on m.id = s."Material_id" 
                        left join mat_grp mg on mg.id = m."MaterialGroup_id"
                        where s."ShipmentHead_id" = sh.id 
					""";

			if (mat_pk != null) {
				sql += " and s.\"Material_id\" = :mat_pk ";
			}

			if (mat_grp_pk != null) {
				sql += " and mg.id = :mat_grp_pk ";
			}

			if (StringUtils.isEmpty(keyword) == false) {
				sql += """
						 and ( m."Name" ilike concat('%%', :keyword,'%%')
						       or m."Code" ilike concat('%%', :keyword,'%%'))
						""";
			}

			sql += """
					)
					       order by sh."ShipDate", c."Name", sh.id
					""";
		}

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
	        , m."id" as item_code
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

	public List<Map<String, Object>> getByLotNumber (String LotNum) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("lotNumber", LotNum);

		String sql = """
				SELECT ml.id,
				               ml.*,
				               m."Name" as material_name,
				               m.id as item_code
				        FROM mat_lot ml
				        JOIN material m
				          ON ml."Material_id" = m.id
				        WHERE ml."LotNumber" = :lotNumber
				""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}
}
