package mes.app.shipment.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import mes.domain.repository.MatLotConsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ShipmentDoBService {

	@Autowired
	SqlRunner sqlRunner;
	@Autowired
	private MatLotConsRepository matLotConsRepository;
	@Autowired
	NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	// 출하지시헤더 조회
	public List<Map<String, Object>> getShipmentHeaderList(String date_from, String date_to, String state, Integer comp_pk, Integer mat_grp_pk, Integer mat_pk, String keyword) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("date_from", Date.valueOf(date_from));
		paramMap.addValue("date_to", Date.valueOf(date_to));
		paramMap.addValue("state", state);
		paramMap.addValue("comp_pk", CommonUtil.tryIntNull(comp_pk));
		paramMap.addValue("mat_grp_pk", CommonUtil.tryIntNull(mat_grp_pk));
		paramMap.addValue("mat_pk", CommonUtil.tryIntNull(mat_pk));
		paramMap.addValue("keyword", keyword == null ? "" : "%" + keyword + "%");

        String sql = """
			with SH as
					    (
					    select sh.id
			                , sh."Company_id" as company_id
			                , c."Name" as company_name
			                , sh."ShipDate" as ship_date	                
			                , sh."TotalPrice" as total_price
			                , sh."TotalVat" as total_vat
			                , sh."State" as state
			                , fn_code_name('shipment_state', sh."State") as state_name
				            , sh."Description" as description
			                from shipment_head sh
				            left join company c on c.id = sh."Company_id"
				            where sh."ShipDate" between :date_from and :date_to		
        		     """;
        
        if (comp_pk != null) {
        	sql += " and sh.\"Company_id\" = :comp_pk ";
        }
        
        if (StringUtils.isEmpty(state) == false) {
        	sql += " and sh.\"State\" = :state ";
        }
        
        sql += """
        		 ), S as 
			    (
			    select s."ShipmentHead_id" as head_id
	            , sum(s."OrderQty") as tot_order_qty
	            , sum(s."Qty") as tot_ship_qty
	            , count(s.id) as item_count
			    from SH
			    inner join shipment s on s."ShipmentHead_id" = SH.id 
        	""";

		if(mat_grp_pk != null || mat_pk != null || !keyword.isEmpty()){
			sql += "inner join material m on m.id = s.\"Material_id\" ";
		}

        sql += " where 1 = 1 ";
        
        if (mat_pk != null) {
        	sql += " and s.\"Material_id\" = :mat_pk ";
        } else if (mat_grp_pk != null) {
        	sql += " and m.\"MaterialGroup_id\" = :mat_grp_pk ";
        }

		if(!keyword.isEmpty()){
			sql += " and m.\"Name\" like :keyword";
		}


        sql += """
        		group by s."ShipmentHead_id"
			    )
			    select SH.*
	            , S.tot_order_qty
	            , S.tot_ship_qty
	            , S.item_count
			    from SH 
			    inner join S on S.head_id = SH.id
	            where 1 = 1
        		""";
		sql += """
				order by SH.ship_date, SH.id desc
				""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
	// 출하 항목 조회
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
	        , COALESCE(suju."InVatYN", 'N') as invat_yn
	        , u."Name" as unit_name 
	        , s."OrderQty"
	        , s."Qty"
	        , COALESCE(suju."Standard", m."Standard1") as standard
	        , s."Description" as description
	        , (select coalesce(sum(mlc."OutputQty" ), 0) as lot_qty from mat_lot_cons mlc where mlc."SourceDataPk" = s.id and mlc."SourceTableName"='shipment') as lot_qty
	        from shipment s 
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id" 
	            inner join material m on m.id = s."Material_id" 
	            left join suju suju on suju.id = s."SourceDataPk" and s."SourceTableName" = 'rela_data'
	            left join mat_grp mg on mg.id = m."MaterialGroup_id" 
	            left join unit u on u.id = m."Unit_id" 
	        where sh.id = :shipment_header_id	
		    order by s.id desc
		        		 """;
		 
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	// 출하 처리 LOT상세
	public List<Map<String, Object>> getShipmentLotList (Integer sh_id, Integer shipment_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("shipment_id", shipment_id);
		paramMap.addValue("sh_id", sh_id);
		
		String sql = """
			          select
		                  mlc.id as mlc_id
		                  , mlc."MaterialLot_id" as ml_id 
		                  , ml."LotNumber"
		                  , ml."CurrentStock"
		                  , ml."OutQtySum"
		                  , mlc."OutputQty"
		                  , ml."Material_id" 
		                  , mlc."SourceDataPk"
		                  , u."Name" as unit_name
		                  , to_char(ml."EffectiveDate", 'YYYY-MM-DD HH24:MI:SS') as "EffectiveDate"
		               from shipment_head sh
				 	   inner join shipment s on s."ShipmentHead_id"=sh.id            
		               inner join mat_lot_cons mlc on mlc."SourceTableName"='shipment' and mlc."SourceDataPk" = s.id
		               inner join mat_lot ml on mlc."MaterialLot_id" = ml.id
		               inner join material m on m.id=ml."Material_id" 
		               left join unit u on u.id =m."Unit_id" 
		               where sh.id = :sh_id
		        		 """;
		if (shipment_id != null) {
			sql += " and s.id = :shipment_id ";
	    }
	    sql += " order by ml.\"LotNumber\" ";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
	// lot 검색
	public List<Map<String, Object>> getMatLotSearch (Integer sh_id, Integer material_id, String lot_number) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();      
		paramMap.addValue("material_id", material_id);
		paramMap.addValue("lot_number", lot_number);
		
		String sql = """
				select\s
				         ml.id as ml_id\s
				        , ml."LotNumber"\s
				        , ml."InputQty"\s
				        , ml."CurrentStock"\s
				        , ml."InputDateTime"        \s
				        , ml."Material_id"
				        , u."Name" as unit_name
				        , m."Code" as mat_code
				        , m."Name" as mat_name
				        , to_char(ml."EffectiveDate", 'YYYY-MM-DD HH24:MI:SS') as "EffectiveDate"
				        , to_char(ml."InputDateTime", 'YYYY-MM-DD HH24:MI:SS') as "InputDateTime"
				from mat_lot ml\s
				    inner join material m on m.id = ml."Material_id"\s
				    left join mat_grp mg on mg.id= m."MaterialGroup_id"\s
				    left join unit u on u.id = m."Unit_id"
				where ml."CurrentStock" > 0\s
		        		 """;
		if (material_id != null) {
			sql += " and ml.\"Material_id\" = :material_id ";
	    }

        if (StringUtils.isEmpty(lot_number) == false) {
        	sql += " and  ml.\"LotNumber\" = :lot_number ";
        }
        
	    sql += " order by ml.\"LotNumber\" ";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public void deleteMatLotCons(Integer mat_lot_cons_id){
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("mat_lot_cons_id", mat_lot_cons_id);

		String sql = """
				delete from mat_lot_cons where id = :mat_lot_cons_id
				""";
		this.sqlRunner.execute(sql, paramMap);
	}

	public void updateShipmentQantityByLotConsume (Integer sh_id, Integer shipment_id, String sourceData) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("sh_id", sh_id);
		paramMap.addValue("shipment_id", shipment_id);
	    
		String sql = """
				with A as(
	            select
	            s.id, coalesce(sum(mlc."OutputQty"),0) as qty
	            from shipment s
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id" 
	            left join mat_lot_cons mlc on mlc."SourceTableName" ='shipment' and mlc."SourceDataPk" = s.id
	            where 1=1 
	            and sh.id = :sh_id
				""";
		
		if (shipment_id != null) {
			sql += " and s.id = :shipment_id ";
		}

		if(sourceData.equals("rela_data")){
			sql += """
				group by s.id),
				UPC as (
	            select
	            s.id
	            , s."Material_id"
	            , sh."Company_id"
	            , mcu."UnitPrice"
	            , m."VatExemptionYN"
	            from A
	            inner join shipment s on s.id = A.id
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id"
	            inner join material m on m.id = s."Material_id" 
	            left join mat_comp_uprice mcu on mcu."Material_id"=s."Material_id" and mcu."Company_id"=sh."Company_id" and mcu."ApplyStartDate" <=now() and mcu."ApplyEndDate" > now()
	            where sh.id = :sh_id 
	        ), B as(        
	           select 
	           s.id
	           , A.qty
	           , UPC."UnitPrice" 
	           , (A.qty * UPC."UnitPrice") as "Price"
	           , case when UPC."VatExemptionYN"='Y' then 0 else (A.qty * UPC."UnitPrice"*0.1) end  as "Vat" 
	           , s."Material_id"
	           , UPC."Company_id"
	           , suju."InVatYN" as invat
	           from shipment s 
	           	
	           	inner join suju suju
				on suju.id = s."SourceDataPk"
				and s."SourceTableName" = 'rela_data'
				
	           	 inner join shipment_head sh2 on sh2.id = s."ShipmentHead_id"
	             inner join A on A.id = s.id             
	             inner join UPC on UPC.id = s.id
	             )
	        update shipment set 
	         "Qty" = B.qty 
	         , "UnitPrice" = B."UnitPrice"
	         , "Price" = CASE
	         		WHEN B.invat = 'Y' THEN ROUND((B."Price" / 1.1)::numeric, 2)
	         		ELSE B."Price"
	         		END
	         , "Vat" = CASE
			    WHEN B.invat = 'Y' THEN ROUND(((B."Price" / 1.1) * 0.1)::numeric, 2)
			    ELSE B."Vat"
			END
	        from B
	        where shipment.id = B.id
	        """;
		}else if(sourceData.equals("product")){
			sql += """
					group by s.id),
				UPC as (
	            select
	            s.id
	            , s."Material_id"
	            , sh."Company_id"
	            , mcu."UnitPrice"
	            , m."VatExemptionYN"
	            from A
	            inner join shipment s on s.id = A.id
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id"
	            inner join material m on m.id = s."Material_id" 
	            left join mat_comp_uprice mcu on mcu."Material_id"=s."Material_id" and mcu."Company_id"=sh."Company_id" and mcu."ApplyStartDate" <=now() and mcu."ApplyEndDate" > now()
	            where sh.id = :sh_id 
	        ), B as(        
	           select 
	           s.id
	           , A.qty
	           , UPC."UnitPrice" 
	           , (A.qty * UPC."UnitPrice") as "Price"
	           , case when UPC."VatExemptionYN"='Y' then 0 else (A.qty * UPC."UnitPrice"*0.1) end  as "Vat" 
	           , s."Material_id"
	           , UPC."Company_id"
	           from shipment s 
	           
	           	 inner join shipment_head sh2 on sh2.id = s."ShipmentHead_id"
	             inner join A on A.id = s.id             
	             inner join UPC on UPC.id = s.id
	             )
	        update shipment set 
	         "Qty" = B.qty 
	         , "UnitPrice" = B."UnitPrice"
	         , "Price" = B."Price"
	         , "Vat" = B."Vat"
			from B
	        where shipment.id = B.id
					""";

		}else{
			throw new RuntimeException("SourceDataTable의 값이 올바르지 않습니다.");
		}
		
        this.sqlRunner.execute(sql, paramMap);    
	}

	public void updateShipmentAndHeadByLotConsume(Integer sh_id, Integer shipment_id, String sourceData) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("sh_id", sh_id);
		paramMap.addValue("shipment_id", shipment_id);

		String sql = """
        with A as (
            select
                s.id,
                coalesce(sum(mlc."OutputQty"), 0) as qty
            from shipment s
            inner join shipment_head sh on sh.id = s."ShipmentHead_id"
            left join mat_lot_cons mlc 
                on mlc."SourceTableName" = 'shipment' 
                and mlc."SourceDataPk" = s.id
            where sh.id = :sh_id
    """;

		if (shipment_id != null) {
			sql += " and s.id = :shipment_id ";
		}

		sql += " group by s.id ), ";

		if (sourceData.equals("rela_data")) {

			sql += """
            UPC as (
                select
                    s.id,
                    s."Material_id",
                    sh."Company_id",
                    --mcu."UnitPrice",
                    coalesce(s."UnitPrice", mcu."UnitPrice") as "UnitPrice",
                    m."VatExemptionYN"
                from A
                inner join shipment s on s.id = A.id
                inner join shipment_head sh on sh.id = s."ShipmentHead_id"
                inner join material m on m.id = s."Material_id"
                left join mat_comp_uprice mcu
                    on mcu."Material_id" = s."Material_id"
                    and mcu."Company_id" = sh."Company_id"
                    and mcu."ApplyStartDate" <= now()
                    and mcu."ApplyEndDate" > now()
                where sh.id = :sh_id
            ),
            B as (
                select
                    s.id,
                    A.qty,
                    UPC."UnitPrice",
                    (A.qty * UPC."UnitPrice" * coalesce(nullif(regexp_replace(suju."Standard", '[^0-9\\.]', '', 'g'), '')::numeric,1)) as "Price",
                    case when UPC."VatExemptionYN" = 'Y' then 0 else (A.qty * UPC."UnitPrice" * 0.1 * coalesce(nullif(regexp_replace(suju."Standard", '[^0-9\\.]', '', 'g'), '')::numeric,1)) end as "Vat",
                    COALESCE(suju."InVatYN", 'N') as invat
                from shipment s
                inner join suju suju on suju.id = s."SourceDataPk" and s."SourceTableName" = 'rela_data'
                inner join A on A.id = s.id
                inner join UPC on UPC.id = s.id
            )
            update shipment s set
                "Qty" = B.qty,
                "UnitPrice" = B."UnitPrice",
                "Price" = case
                    when B.invat = 'Y' then ROUND((B."Price" / 1.1)::numeric, 2)
                    else B."Price"
                end,
                "Vat" = case
                    when B.invat = 'Y' then ROUND(((B."Price" / 1.1) * 0.1)::numeric, 2)
                    else B."Vat"
                end
            from B
            where s.id = B.id;

            update shipment_head sh set
                "TotalQty" = coalesce((select sum(s."Qty") from shipment s where s."ShipmentHead_id" = :sh_id), 0),
                "TotalPrice" = coalesce((select sum(s."Price") from shipment s where s."ShipmentHead_id" = :sh_id), 0),
                "TotalVat" = coalesce((select sum(s."Vat") from shipment s where s."ShipmentHead_id" = :sh_id), 0)
            where sh.id = :sh_id;
        """;

		} else if (sourceData.equals("product")) {

			sql += """
            UPC as (
                select
                    s.id,
                    s."Material_id",
                    sh."Company_id",
                    mcu."UnitPrice",
                    m."VatExemptionYN"
                from A
                inner join shipment s on s.id = A.id
                inner join shipment_head sh on sh.id = s."ShipmentHead_id"
                inner join material m on m.id = s."Material_id"
                left join mat_comp_uprice mcu 
                    on mcu."Material_id" = s."Material_id"
                    and mcu."Company_id" = sh."Company_id"
                    and mcu."ApplyStartDate" <= now()
                    and mcu."ApplyEndDate" > now()
                where sh.id = :sh_id
            ),
            B as (
                select
                    s.id,
                    A.qty,
                    UPC."UnitPrice",
                    (A.qty * UPC."UnitPrice") as "Price",
                    case when UPC."VatExemptionYN" = 'Y' then 0 else (A.qty * UPC."UnitPrice" * 0.1) end as "Vat"
                from shipment s
                inner join A on A.id = s.id
                inner join UPC on UPC.id = s.id
            )
            update shipment s set
                "Qty" = B.qty,
                "UnitPrice" = B."UnitPrice",
                "Price" = B."Price",
                "Vat" = B."Vat"
            from B
            where s.id = B.id;

            update shipment_head sh set
                "TotalQty" = coalesce((select sum(s."Qty") from shipment s where s."ShipmentHead_id" = :sh_id), 0),
                "TotalPrice" = coalesce((select sum(s."Price") from shipment s where s."ShipmentHead_id" = :sh_id), 0),
                "TotalVat" = coalesce((select sum(s."Vat") from shipment s where s."ShipmentHead_id" = :sh_id), 0)
            where sh.id = :sh_id;
        """;
		} else {
			throw new RuntimeException("SourceDataTable의 값이 올바르지 않습니다.");
		}

		this.sqlRunner.execute(sql, paramMap);
	}

	// 수주헤더 기준으로 출하항목(shipment) 금액합산 정리
	public void updateShipmentStateComplete (Integer sh_id, String description, String sourceData) {

		updateShipmentQantityByLotConsume(sh_id, null, sourceData);

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("sh_id", sh_id);
		paramMap.addValue("description", description);

		String sql = """
				with A as(
				select
		        sh.id as sh_id
		        , count(s.id) as s_count
		        , sum(s."Price") as "TotalPrice"
		        , sum(s."Vat") as "TotalVat"
		        , sum(s."Qty") as "TotalQty"
		        from shipment s
		        inner join shipment_head sh on sh.id=s."ShipmentHead_id"
		        where sh.id=:sh_id
		        group by sh.id
		        )
		        update
		        shipment_head
		        set
		        --이거 lot 추가할때마다 head도 합산해서 수정해줌 그래서 아래는 주석함.
		        --"TotalQty" = A."TotalQty"
		        --,"TotalVat" = A."TotalVat"
		        --, "TotalPrice" = A."TotalPrice"
		        "State" = 'shipped'
		        ,"Description" = :description
		        from A
		        where id=A.sh_id
				""";

        this.sqlRunner.execute(sql, paramMap);  
	}
	
	// 관련 수주를 찾아서 수주의 출하 상태를 변경한다.
	public void updateSujuShipmentState (Integer sh_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("sh_id", sh_id);
	    
		String sql = """
		        with A as(
		        select
		        s.id as shipment_id
		        ,sh.id as sh_id
		        , rd."DataPk1" as suju_id
		        , sj."State"
		        , sj."ShipmentState"
		        from shipment s 
		        inner join shipment_head sh on sh.id=s."ShipmentHead_id"
		        inner join rela_data rd on rd."TableName1" ='suju' and rd."TableName2" ='shipment' and rd."DataPk2" =s.id
		        inner join suju sj on sj.id = rd."DataPk1" 
		        where sh.id = :sh_id
		        )
		        update suju set "ShipmentState" ='shipped'
		        from A where A.suju_id = id
				""";
		
        this.sqlRunner.execute(sql, paramMap); 
	}


}
