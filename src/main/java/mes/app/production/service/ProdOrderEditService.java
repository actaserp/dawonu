package mes.app.production.service;

import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import mes.Exception.CustomException;
import mes.app.definition.service.BomService;
import mes.app.production.Enum.ProcessType;
import mes.app.production.production_package.*;
import mes.app.production.production_package.ProductionStrategy.ProcessStartStrategy;
import mes.app.util.JsonUtil;
import mes.domain.entity.JobRes;
import mes.domain.entity.JobResProcessTree;
import mes.domain.entity.Material;
import mes.domain.entity.User;
import mes.domain.repository.JobResProcessTreeRepository;
import mes.domain.repository.JobResRepository;
import mes.domain.repository.MaterialRepository;
import mes.domain.services.CommonUtil;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;

@Service
public class ProdOrderEditService {

    private final SqlRunner sqlRunner;
    private final MaterialRepository materialRepository;
    private final JobResRepository jobResRepository;
    private final Map<ProcessType, ProcessStartStrategy> strategyMap;
    private final BomService bomService;
    private final JobResProcessTreeRepository jobResProcessTreeRepository;
    private final EntityManager entityManager;

    public ProdOrderEditService(SqlRunner sqlRunner, MaterialRepository materialRepository, JobResRepository jobResRepository, List<ProcessStartStrategy> strategies, BomService bomService, JobResProcessTreeRepository jobResProcessTreeRepository, EntityManager entityManager) {
        this.sqlRunner = sqlRunner;
        this.materialRepository = materialRepository;
        this.jobResRepository = jobResRepository;
        this.strategyMap = strategies.stream()
                .collect(Collectors.toMap(
                        ProcessStartStrategy::getType,
                        Function.identity()
                ));
        this.bomService = bomService;
        this.jobResProcessTreeRepository = jobResProcessTreeRepository;
        this.entityManager = entityManager;
    }


    // 수주 목록 조회
		public List<Map<String, Object>> getSujuList(String date_kind, String start, String end, Integer mat_group, String mat_name, String not_flag, String spjangcd, Integer cboFactory) {

			MapSqlParameterSource dicParam = new MapSqlParameterSource();
			dicParam.addValue("start", Timestamp.valueOf(start + " 00:00:00"));
			dicParam.addValue("end", Timestamp.valueOf(end + " 23:59:59"));
			dicParam.addValue("mat_group", mat_group);
			dicParam.addValue("mat_name", mat_name);
			dicParam.addValue("cboFactory", cboFactory);
			dicParam.addValue("spjangcd", spjangcd);

			if (StringUtils.isEmpty(date_kind)) {
				date_kind = "sales";
			}

			String sql = """
        with s as (
            select
                s.id,
                s."JumunDate",
                s."DueDate",
                s."JumunNumber",
                s."CompanyName",
                s."Material_id",
                s."Standard",
                s."SujuQty",
                s."SujuQty2",
                coalesce(s."ReservationStock", 0) as "ReservationStock",
                fn_code_name('suju_state', s."State") as "StateName",
                s."State",
                s."Description" as description,
                s."Material_Name" as suju_mat_name
            from suju s
            where 1 = 1
              and s.spjangcd = :spjangcd
        """;

			if ("suju_date".equals(date_kind)) {
				sql += " and s.\"JumunDate\" between :start and :end ";
			} else {
				sql += " and s.\"DueDate\" between :start and :end ";
			}

			// ✅ 품명 검색: 매칭/미매칭 둘 다 대응 (material.Name 우선, 없으면 suju.Material_Name)
			if (StringUtils.isEmpty(mat_name) == false) {
				sql += """
            and (
                upper(coalesce(s."Material_Name", '')) like concat('%%', upper(:mat_name), '%%')
                or upper(coalesce(s."Material_Name", '')) = upper(:mat_name)
            )
        """;
			}

			sql += """
        )
        , sm as (
            select
                s.*,
                (s."Material_id" is not null) as is_matched,
                m."Code" as mat_code,
                m."WorkCenter_id" as workcenter_id,
                coalesce(m."Name", s.suju_mat_name) as mat_name,
                u."Name" as unit_name,
                mg."Name" as "MaterialGroupName",
                mg.id as "MaterialGroup_id",
                fn_code_name('mat_type', mg."MaterialType") as mat_type_name,
                f."Name" as fac_name,
                r."Name" as routing_nm
            from s
            left join material m on m.id = s."Material_id"
            left join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id"
            left join factory f on f.id = m."Factory_id"
            left join routing r on r.id = m."Routing_id"
            where 1 = 1
            -- sm.is_matched 대신 s."Material_id" 직접 체크
            and (s."Material_id" is null or mg."MaterialType" is not null)
            and (s."Material_id" is null or mg."MaterialType" in ('semi','product'))

        )
        , q as (
            select
                sm.id as suju_id,
                jr."Description" as memo,
                sum(jr."OrderQty") as ordered_qty,
                sum(sum(jr."OrderQty")) over (partition by sm.id) as total_ordered_qty
            from job_res jr
            inner join sm
                on sm.id = jr."SourceDataPk"
               and jr."SourceTableName" = 'suju'
            where jr."State" <> 'canceled'
             and jr."Parent_id" is null
            group by sm.id, jr."Description"
        )
        select
            sm.id,
            sm."JumunNumber",
            to_char(sm."JumunDate", 'yyyy-mm-dd') as "JumunDate",
            to_char(sm."DueDate", 'yyyy-mm-dd') as "DueDate",
            sm."CompanyName",
            sm."Standard",

            sm."Material_id" as mat_pk,
            sm."SujuQty" as "SujuQty",
            sm."SujuQty2" as "SujuQty2",
            sm."ReservationStock" as "ReservationStock",

            coalesce(q.ordered_qty, 0) as ordered_qty,
            round(greatest(0, sm."SujuQty2" - coalesce(q.total_ordered_qty, 0))::numeric, 2) as remain_qty,
            0 as "AdditionalQty",

            sm.description,
            sm."StateName",
            sm."State",

            --매칭/미매칭 구분 + 제품명
            sm.is_matched,
            sm.mat_name,

            -- 매칭된 경우만 채워지는 컬럼들
            sm.unit_name,
            sm.workcenter_id,
            sm.mat_type_name,
            sm."MaterialGroupName",
            sm.mat_code,
            sm.routing_nm,
            sm.fac_name,

            q.memo
        from sm
        left join q on q.suju_id = sm.id
        where 1 = 1 
        """;

			// not_flag (잔여량 > 0)
			if (StringUtils.isEmpty(not_flag) == false) {
				sql += " and (sm.\"SujuQty2\" - coalesce(q.total_ordered_qty, 0)) > 0 ";
			}

			if (cboFactory != null) {
				sql += " and sm.is_matched = true and sm.fac_name is not null and (select m2.\"Factory_id\" from material m2 where m2.id = sm.\"Material_id\") = :cboFactory ";
			}
			if (mat_group != null) {
				sql += " and sm.is_matched = true and sm.\"MaterialGroup_id\" = :mat_group ";
			}

			if ("suju_date".equals(date_kind)) {
				sql += " order by sm.\"DueDate\" desc, sm.\"JumunNumber\" desc ";
			} else {
				sql += " order by sm.\"JumunDate\" desc, sm.\"JumunNumber\" desc ";
			}

			return this.sqlRunner.getRows(sql, dicParam);
		}


	public List<Map<String, Object>> makeProdOrder(Integer sujuId) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("sujuId", sujuId);

        String sql = """
                with qsum as(
                select
                s.id as suju_id
                , s."Material_id"
                , s."SujuQty"
                , (select sum(jr."OrderQty") from job_res jr where jr."SourceDataPk" = s.id and jr."SourceTableName"='suju' and jr."State" <>'canceled' ) as ordered_qty
                from suju s
                where s.id = :sujuId
                )
                select suju_id, "Material_id", "SujuQty", ordered_qty, greatest("SujuQty"-ordered_qty, 0 ) as remain_qty
                from qsum
				""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }

    // 제품 지시내역 조회
    public List<Map<String, Object>> getJobOrderList(Integer suju_id) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("suju_id", suju_id);

        String sql = """
			select jr.id
			, jr."WorkOrderNumber"
			, jr."ProductionDate"
			, jr."ShiftCode" 
			, s."Name" as "ShiftName"
			, m."Code" as mat_code
			, m."Name" as mat_name
			, u."Name" as unit_name
			, ROUND(jr."OrderQty"::numeric, 2) as "OrderQty"
			, jr."WorkCenter_id" 
			, wc."Name" as "WorkcenterName"
			, jr."Equipment_id"
			, e."Name" as "EquipmentName"
			, jr."State" 
			, fn_code_name('job_state', jr."State") as "StateName"
			, sju."Standard" as standard
			, sju.id as suju_id
			, jr."Description"
			, sju."DueDate" 
			from job_res jr 
			inner join material m on m.id = jr."Material_id" 
			inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
			left join unit u on u.id = m."Unit_id" 
			left join shift s on s."Code" = jr."ShiftCode" 
			left join work_center wc on wc.id = jr."WorkCenter_id"
			left join equ e on e.id = jr."Equipment_id"
			LEFT JOIN suju sju ON sju.id = jr."SourceDataPk"
			where jr."SourceDataPk"=:suju_id
			and jr."SourceTableName" ='suju'
			order by jr."WorkOrderNumber" desc, jr.id
			""";

        List<Map<String, Object>> job_res = this.sqlRunner.getRows(sql, dicParam);

        String sql_suju_detail = """
			SELECT
				sd.id,
				sd."suju_id",
				sd."Standard",
				sd."Qty"
			FROM suju_detail sd
			WHERE sd."suju_id" = :suju_id
			ORDER BY sd.id
		""";

        List<Map<String, Object>> suju_detail = this.sqlRunner.getRows(sql_suju_detail, dicParam);

        for (Map<String, Object> job : job_res) {
            job.put("items", suju_detail);
        }

        return job_res;
    }


    // 제품 지시내역 상세조회
    public Map<String, Object> getJobOrderDetail(Integer jobres_id) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("jobres_id", jobres_id);

        String sql = """
				select jr.id
	            , jr."WorkOrderNumber"
	            , to_char(jr."ProductionDate", 'yyyy-mm-dd') as "ProductionDate"
	            , jr."Material_id"
	            , jr."ShiftCode" 
	            , s."Name" as "ShiftName"
	            , m."Name" as mat_name
	            , u."Name" as unit_name
	            , ROUND(jr."OrderQty"::numeric, 2) as "OrderQty"
	            , jr."WorkCenter_id" 
	            , jr."Equipment_id"
	            , jr."State" 
	            , fn_code_name('job_state', jr."State") as "StateName"
	            , jr."Description"
	            from job_res jr 
	            inner join material m on m.id = jr."Material_id" 
	            left join unit u on u.id = m."Unit_id" 
	            left join shift s on s."Code" = jr."ShiftCode" 
	            left join work_center wc on wc.id = jr."WorkCenter_id"
	            where jr.id = :jobres_id
			""";

        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }

    // 반제품 작업지시 조회
    public List<Map<String, Object>> getSemiList(String data_date, Integer mat_pk, Double suju_qty, Integer suju_pk) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("data_date", data_date);
        dicParam.addValue("mat_pk", mat_pk.toString());
        dicParam.addValue("mat_order_qty", suju_qty);
        dicParam.addValue("suju_pk", suju_pk);

        String sql = """
				with A as (
	                select m.id as mat_pk
	                , mg."Name" as group_name
	                , m."Name" as mat_name
	                , m."Code" as mat_code
	                , m."WorkCenter_id" as workcenter_id
	                , m."StoreHouse_id" as storehouse_id
	                , u."Name" as unit_name
	                , fn_unit_ceiling( bom.bom_ratio * :mat_order_qty, u."PieceYN" ) as bom_qty
	                , fn_unit_ceiling( bom.bom_ratio * :mat_order_qty, u."PieceYN" ) as order_qty
	                from tbl_bom_detail(:mat_pk, :data_date) as bom
	                inner join material m on m.id = bom.mat_pk
	                left join unit u on u.id = m."Unit_id"
	                inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
	                where mg."MaterialType" in ('semi')
	                ), 
	                sq as (                
					select 
					 s.id as suju_pk
					 ,jr."Material_id" as mat_pk
					 , sum(jr."OrderQty") as ordered_qty
					from job_res jr 
					 inner join suju s on s.id=jr."SourceDataPk" and jr."SourceTableName" ='suju'
					 inner join material m on m.id=jr."Material_id" 
					 inner join mat_grp mg on mg.id=m."MaterialGroup_id"  
					where 
					s.id = :suju_pk
					and mg."MaterialType" ='semi'
					group by s.id, jr."Material_id" 
	                )
	                select A.*, sq.ordered_qty
	                from A
	                left join sq on sq.mat_pk = A.mat_pk
				""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }

    // 반제품 지시내역 조회
    public List<Map<String, Object>> getSemiJoborderList(Integer suju_id) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("suju_id", suju_id);

        String sql = """
				select jr.id
	            , jr."WorkOrderNumber"
	            , jr."ProductionDate"
	            , jr."ShiftCode" 
	            , s."Name" as "ShiftName"
	            , m."Code" as mat_code
	            , m."Name" as mat_name
	            , u."Name" as unit_name
	            , jr."OrderQty" as "OrderQty"
	            , jr."WorkCenter_id" 
	            , wc."Name" as "WorkcenterName"
	            , jr."Equipment_id"
	            , e."Name" as "EquipmentName"
	            , jr."State" 
	            , fn_code_name('job_state', jr."State") as "StateName"
	            from job_res jr 
	            inner join material m on m.id = jr."Material_id" 
	            inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
	            left join unit u on u.id = m."Unit_id" 
	            left join shift s on s."Code" = jr."ShiftCode" 
	            left join work_center wc on wc.id = jr."WorkCenter_id"
	            left join equ e on e.id = jr."Equipment_id"
	            where jr."SourceDataPk"=:suju_id 
	            and jr."SourceTableName" ='suju'
	            and mg."MaterialType" in ('semi')
	            order by jr."WorkOrderNumber" desc, jr.id
				""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }
    /// ///////////////////////////////////////////////////////////////// 작업지시 생성 ///////////////////////////////////
    //region : 작업지시 생성
    public JobRes makeParentProdOrder(
            Integer sujuId,
            String productionDate,
            Integer materialId,
            String cboShiftCode,
            Integer cboWorcenter,
            Integer cboEquiment,
            Float txtOrderQty,
            String spjangcd, User user
    ){

        Material m = materialRepository.getMaterialById(materialId);

        Timestamp prodDate = CommonUtil.tryTimestamp(productionDate);

        JobRes header = new JobRes();

        //부모 작업지시 생성.
        header.set_audit(user);
        header.setProductionDate(prodDate);
        header.setProductionPlanDate(prodDate);
        header.setMaterialId(materialId);
        header.setOrderQty(txtOrderQty);
        header.setStoreHouse_id(m.getStoreHouseId());
        header.setLotCount(1);
        header.setState("ordered");
        header.setSourceDataPk(sujuId);
        header.setSourceTableName("suju");
        header.setSpjangcd(spjangcd);
        header.setWorkCenter_id(cboWorcenter);
        header.setFirstWorkCenter_id(cboWorcenter);
        header.setEquipment_id(cboEquiment);
        header.setShiftCode(cboShiftCode);

        // 첫 공정 판단 객체 생성 (작지 내려야 하는데 어떤 공정을 첫 작지로 내릴까?)
        ProcessFlow flow = ProcessFlow.from(m);

        header.setProcessCount(flow.cnt());

        if(flow.startType() == ProcessType.FULL_FLOW){
            header.setWorkIndex(flow.cnt() + 1); //얘가 마지막 공정이니 얘한테 딸린 공정을 다음이겠지
        }else{
            header.setWorkIndex(flow.cnt());
        }

        //전략 선택
        ProcessType type = flow.startType();
        ProcessStartStrategy strategy = strategyMap.get(type);

        if(strategy == null) throw new CustomException("공정 시작 전략이 존재하지 않습니다.");

        //전체 bom 조회
        List<Map<String, Object>> bomListByMat = bomService.getBomListByMat(materialId.toString());

        //전체 bom -> json 형태로 보기쉽게 변환
        BomTreeService bomTreeService = new BomTreeService();

        // 1. bom 트리 생성
        Map<String, BomNode> bomTree =  bomTreeService.buildTree(bomListByMat);

        //처음 작업에 대한 품목 판별 및 현재공정판별
        bomTreeService.markFirstCurrentProcess(flow, bomTree, header.getOrderQty());

        //부모(마스터 작지) 저장
        header = jobResRepository.save(header); //트리거가 번호 생성
        jobResRepository.flush(); //WorkOrderNum을 알아야함. 근데 이거 트리거라서 flush , flush여도 롤백쌉가능
        entityManager.refresh(header);

        JobRes savedHeader = jobResRepository.findById(header.getId()).orElseThrow(() -> new CustomException("엔티티를 찾을 수 없습니다."));

        //job_res_process_tree에 저장
        saveJobResProcessTree(savedHeader, JsonUtil.mapToJson(bomTree));


        ///  얘는 전략패턴임. 분기 if 제거용이 아니라 도메인 구조에 따라 동적인 코드를 구성하기 위한것
        /// 호출 클라이언트에서는 상세코드를 몰라도 된다. 그저 어떠한 공정인지만 알아내서 던져주면
        ///  내부에서 알아서 처리할 수 있게끔.
        //자식 작업지시 생성 (1차 or 3차 단독이면 3차)
        Map<ProcessType, BomNode> bomNode = resolveStartBomNodes(bomTree);
        strategy.start(header, flow, bomNode, user);

        //TODO: 트랜잭션 처리는 문제가 없는지?

        return header;
    }

    private void saveJobResProcessTree(JobRes jr, String bomTree) {

        JobResProcessTree jpt = new JobResProcessTree();

        jpt.setWorkOrderNo(jr.getWorkOrderNumber());
        jpt.setProcessTree(bomTree);

        jobResProcessTreeRepository.save(jpt);
    }

    /**
     * 첫 작업지시 생성을 위한 시작 BomNode 목록 반환
     * - 1·2차 메인라인 → 1개
     * - 3차 공정 포함 시 → 병렬로 추가
     */
    private Map<ProcessType, BomNode> resolveStartBomNodes(
            Map<String, BomNode> bomTree
    ) {
        if (bomTree == null || bomTree.isEmpty()) {
            throw new IllegalArgumentException("BOM tree is empty");
        }

        Map<ProcessType, BomNode> result = new EnumMap<>(ProcessType.class);

        // 1️⃣ 메인 라인 (1·2차)
        BomNode main = bomTree.get(ProcessType.SIMPLE_FLOW.name());
        if (main != null) {
            result.put(ProcessType.SIMPLE_FLOW, main);
        }

        // 2️⃣ 3차 병렬 공정
        BomNode third = bomTree.get(ProcessType.FULL_FLOW.name());
        if (third != null) {
            result.put(ProcessType.FULL_FLOW, third);
        }

        // 3️⃣ fallback: 키가 달라도 하나뿐인 경우
        if (result.isEmpty() && bomTree.size() == 1) {
            BomNode only = bomTree.values().iterator().next();
            result.put(ProcessType.SIMPLE_FLOW, only); // 의미상 메인으로 간주
        }

        if (result.isEmpty()) {
            throw new IllegalStateException("Cannot resolve start BomNodes from bomTree");
        }

        return result;
    }

	public List<Map<String, Object>> searchMatMatch(String keyword, String spjangcd) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("keyword", keyword);
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
			select
			 	  m.id
				, fn_code_name('mat_type', mg."MaterialType" ) as mat_type_name
				, mg."Name" as mat_grp_name
				, m."Code" as mat_code
				, m."Name" as mat_name
				, m."Class1" as class1 --1차공정
				, uc1."Value" as "class1Name"
				, m."Class2" as class2 --2차공정
				, uc2."Value" as "class2Name"
				, m."Class3" as class3 --3차공정
				, uc3."Value" as "class3Name"
				, m."WorkCenter_id" 
				, wc."Name" as workcenter_name
				, m."StoreHouseLoc" as storehouse_loc
				from material m
				left join mat_grp mg on mg.id = m."MaterialGroup_id"
				left join work_center wc on wc.id = m."WorkCenter_id"
				left join user_code uc1 on m."Class1" = uc1."Code"
				left join user_code uc2 on m."Class2" = uc2."Code"
				left join user_code uc3 on m."Class3" = uc3."Code"
				where 1=1
				AND m.spjangcd = :spjangcd
				AND m."Useyn" = '0'
				and ( m."Name" ilike concat('%',:keyword,'%') or m."Code" ilike concat('%',:keyword,'%'))
				and mg."MaterialType" in('semi','product' ); -- 반제품, 제품만 조회
		""";
		return sqlRunner.getRows(sql, dicParam);
		}

	@Transactional
	public void saveMatMatch(Integer sujuItemId, Integer materialId, String spjangcd, User user) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("suju_item_id", sujuItemId);
		param.addValue("material_id", materialId);
		param.addValue("spjangcd", spjangcd);

		String sql = """
        update suju
           set "Material_id" = :material_id,
               _modified = now(),
               _modifier_id = :user_id
         where id = :suju_item_id
           and spjangcd = :spjangcd
    """;

		param.addValue("user_id", user.getId());

		int updated = sqlRunner.execute(sql, param); // ← 너희 프로젝트 execute 메서드명에 맞게 수정

		if (updated == 0) {
			throw new CustomException("매칭 저장 실패(대상 수주가 없거나 사업장코드 불일치).");
		}
	}


    /*public JobRes makeJobRes(Integer materialId) {

        Material m = materialRepository.getMaterialById(materialId);

        // 첫 공정 판단 객체 생성 (작지 내려야 하는데 어떤 공정을 첫 작지로 내릴까?)
        ProcessFlow flow = ProcessFlow.from(m);

        ProcessType type = null;
        if(flow.hasThirdProcess()){
            type = ProcessType.FULL_FLOW;
        }else{
            type = ProcessType.SIMPLE_FLOW;
        }


        if(1==1) throw new CustomException("asda");

        return null;
    }*/

    //endregion


}
