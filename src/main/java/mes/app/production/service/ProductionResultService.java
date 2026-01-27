package mes.app.production.service;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.IntStream;
import mes.Exception.CustomException;
import mes.app.inventory.service.LotService;
import mes.app.production.ProductuibResult_validation.ProductionResultValidator;
import mes.app.production.dto.productionResult.WorkFinishRequest;
import mes.app.production.production_package.BomNode;
import mes.app.production.production_package.BomTreeService;
import mes.app.util.JsonUtil;
import mes.app.util.UtilClass;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.*;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import mes.domain.services.SqlRunner;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;



@Service
public class ProductionResultService {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	StorehouseRepository storehouseRepository;

	@Autowired
	MatLotConsRepository matLotConsRepository;

	@Autowired
	MatLotRepository matLotRepository;

    @Autowired
    ProductionResultValidator validator;

    @Autowired
    MatConsuRepository matConsuRepository;

    @Autowired
    EquRunRepository equRunRepository;

    @Autowired
    JobResRepository jobResRepository;

    @Autowired
    MatProduceRepository matProduceRepository;

    @Autowired
    MaterialRepository materialRepository;

    @Autowired
    MaterialGroupRepository materialGroupRepository;

    @Autowired
    WorkcenterRepository workcenterRepository;

    @Autowired
    MatInoutRepository matInoutRepository;

    @Autowired
    LotService lotService;

    @Autowired
    JobResProcessTreeRepository jobResProcessTreeRepository;

    /**
     * 작업지시에서 발생한 불량 수량을
     * 불량 전용 창고로 입고 처리하여 재고에 반영한다.
     *
     * @param jrPk 작업지시 ID
     * @param id   처리 기준 ID
     */
	public void add_jobres_defectqty_inout(Integer jrPk, int id) {

		List<StoreHouse> sh = this.storehouseRepository.findByHouseType("defect");
		Integer defectHousePk = null;
		if (sh.size() > 0) {
			defectHousePk = sh.get(0).getId();
		} else {
			return;
		}

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);
		dicParam.addValue("housePk", defectHousePk);
		dicParam.addValue("userId", id);

		String sql = """
				 insert into mat_inout ("Material_id","StoreHouse_id", "InoutDate", "InoutTime", "InOut", "InputType"
               , "InputQty", "Description", "SourceDataPk", "SourceTableName", "State", _status, _created, _creater_id)
               select jr."Material_id"
               , :housePk
               , now()::date as "InoutDate"
               , now()::time as "InoutTime"
               ,'in' as "InOut"
               ,'produced_in' as "InputType"
               , jrd."DefectQty" as "InputQty"
               , dt."Name" as "Description"
               , jrd.id as "SourceDataPk"
               , 'job_res_defect' as "SourceTableName"
               , 'confirmed' as status
               , 'a' as _status
               , now() as _created
               , :userId as _creater_id
               from job_res_defect jrd 
               inner join job_res jr on jr.id=jrd."JobResponse_id"
               left join defect_type dt on dt.id = jrd."DefectType_id" 
               where jrd."DefectQty" > 0 
               and jrd."JobResponse_id" = :jrPk
				""";

		this.sqlRunner.execute(sql, dicParam);
	}

	public void delete_jobres_defectqty_inout(Integer jrPk) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);

		String sql = """
				delete from mat_inout
		        where "SourceTableName"='job_res_defect'
		        and "SourceDataPk" in (select id
	            from job_res_defect
	            where "JobResponse_id" = :jrPk)
				""";
		this.sqlRunner.execute(sql, dicParam);

	}



	public void calculate_balance_mat_lot_with_job_res(int id) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);

		String sql = """
                      with ll as(
                      select
                      ml.id as ml_id
                      from job_res jr
                      inner join mat_proc_input mpi on mpi."MaterialProcessInputRequest_id"=jr."MaterialProcessInputRequest_id"
                      inner join mat_lot ml on ml.id = mpi."MaterialLot_id"
                      where jr.id = :id
                      ),
                
                      ss as( 
                      select 
                      ll.ml_id, 
                      sum(mlc."OutputQty") as out_sum 
                      from ll 
                      left join mat_lot_cons mlc on ll.ml_id= mlc."MaterialLot_id" 
                      group by ll.ml_id
                      ), 
                      
                      T as(
                      select 
                      ss.ml_id, coalesce(ss.out_sum,0) as out_sum, 
                      ml."InputQty" 
                      from ss
                      inner join mat_lot ml on ml.id=ss.ml_id
                      )
                      
                      update mat_lot set "OutQtySum" = T.out_sum
                      , "CurrentStock" = mat_lot."InputQty"-T.out_sum
                      from T 
                      where T.ml_id = mat_lot.id
                """;

		this.sqlRunner.execute(sql, dicParam);
	}

	public void delete_mlc_and_rebalance_ml(int id) {
		List<MatLotCons> mcList = this.matLotConsRepository.findBySourceTableNameAndSourceDataPk("mat_produce", id);

		for (int i = 0; i < mcList.size(); i++) {
			MaterialLot ml = this.matLotRepository.getMatLotById(mcList.get(i).getMaterialLotId());
			Integer mId = ml.getId();
			this.matLotConsRepository.deleteById(mcList.get(i).getId());

			MapSqlParameterSource dicParam = new MapSqlParameterSource();
			dicParam.addValue("mId", mId);

			String sql = """
                             with SS as (
                             select 
                             ml.id as ml_id, sum("OutputQty") as out_qty_sum
                             from mat_lot_cons mlc 
                             inner join mat_lot ml on ml.id = mlc."MaterialLot_id"   
                             where ml.id= :mId
                             group by ml.id
                             )        
                             update mat_lot set 
                              "CurrentStock" = mat_lot."InputQty"-COALESCE(ss.out_qty_sum,0)
                              , "OutQtySum" = COALESCE(ss.out_qty_sum,0)
                              , _modified = now()
                             from ss
                             where ss.ml_id = mat_lot.id
                    """;


			this.sqlRunner.execute(sql, dicParam);
		}
	}

	public void calculate_balance_mat_lot_with_mat_prod(int id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mpId", id);

		String sql = """
                   with MS as (
                 	    select 
                      ml.id, sum(mlc."OutputQty") as "OutQtySum"
                      from mat_lot_cons mlc 
                      inner join mat_lot ml on ml.id = mlc."MaterialLot_id"
                      inner join mat_produce mp on mp.id= mlc."SourceDataPk" and mlc."SourceTableName" ='mat_produce'
                      where mlc."SourceDataPk"= :mpId
                      group by ml.id 
                      )
                      update mat_lot set 
                      "CurrentStock" = mat_lot."InputQty"-COALESCE(MS."OutQtySum",0)
                      , "OutQtySum" = MS."OutQtySum"
                      , _modified = now()
                      from MS
                      where MS.id = mat_lot.id
                """;

		this.sqlRunner.execute(sql, dicParam);
	}

	public List<Map<String, Object>> getProdResult(String dateFrom, String dateTo, String isIncludeComp, String spjangcd, String choMat, Integer cboFactory, Integer job_proc) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("dateFrom", dateFrom);
		dicParam.addValue("dateTo", dateTo);
		dicParam.addValue("isIncludeComp", isIncludeComp);
		dicParam.addValue("spjangcd", spjangcd);
		dicParam.addValue("cboFactory", cboFactory);
		dicParam.addValue("matName", (choMat.isEmpty() || choMat == null) ? "%%": "%" + choMat  +"%");
        dicParam.addValue("jobProc", job_proc);

        String sql = """
                WITH T AS (
                			  SELECT
                				  jr.id                              AS child_id,
                				  jr."Parent_id"                     AS parent_id,
                				  jr."Description" 					 as memo ,
                				  COALESCE(jr."Parent_id", jr.id)    AS base_id,
                				  CASE WHEN jr."State"='working' THEN 1 ELSE 0 END AS is_working,
                				  CASE WHEN jr."State"='stopped' THEN 1 ELSE 0 END AS is_stopped
                			  FROM job_res jr
                			  WHERE jr."ProductionDate" BETWEEN CAST(:dateFrom AS date) AND CAST(:dateTo AS date)
                				AND jr.spjangcd = :spjangcd
                			),
                			S AS (
                              SELECT
                            	  T.*,
                            	  -- 대표행 선택: working 우선, 다음 최근 id
                            	  ROW_NUMBER() OVER (
                            			PARTITION BY T.base_id
                            			ORDER BY
                            			  T.is_working DESC,                                  -- 1) working 우선
                            			  CASE WHEN T.parent_id IS NULL THEN 1 ELSE 0 END DESC, -- 2) 그다음 부모 우선
                            			  T.child_id DESC                                     -- 3) 마지막 타이브레이커: 최신 id
                            		  ) AS rn,
                            	  -- 체인에 working 있는지 (있으면 1)
                            	  MAX(T.is_working) OVER (PARTITION BY T.base_id) AS any_working
                            	  , MAX(T.is_stopped) OVER (PARTITION BY T.base_id) AS any_stopped
                              FROM T
                            ),
                			F AS (
                			SELECT
                			S.child_id as id
                			, C."WorkOrderNumber"                         AS order_num
                			, TO_CHAR(C."ProductionDate",'yyyy-mm-dd')    AS prod_date  --생산일
                			, TO_CHAR(su."DueDate",'yyyy-mm-dd')    AS due_date         --수주테이블의 마감기한
                			, TO_CHAR(su."DueDate",'yyyy-mm-dd')    AS due_date         --수주테이블의 마감기한
                			, C."LotNumber"                               AS lot_num    --lot 번호
                			, TO_CHAR(C."StartTime",'hh24:mi')            AS start_time --작업 시작시간
                			, TO_CHAR(C."EndTime",'hh24:mi')              AS end_time   --작업 종료시간
                			, WC.id                                       AS workcenter_id --작업 워크센터 아이디
                			, WC."Name"                                   AS workcenter    --작업 웨크센터 이름
                			, C."ShiftCode"                                AS shift_code   --근무조 코드
                			, SH."Name"                                    AS shift_name   --근무조 이름
                			, C."WorkIndex"                                AS work_idx     --작업순서
                            -- 파생 상태: working 있으면 working, 아니면 부모 상태
                            , CASE
                            		 WHEN S.is_working = 1 THEN 'working'
                            		 WHEN S.is_working = 1 THEN 'stopped'
                            		 ELSE C."State"
                            	 END AS state
                            	 , fn_code_name('job_state',
                            		 CASE
                            			 WHEN S.is_working = 1 THEN 'working'
                            			 WHEN S.is_working = 1 THEN 'stopped'
                            			 ELSE C."State"
                            		 END
                            ) AS job_state                 --작업상태 이름
                			, C."WorkerCount"                              AS worker_count  --작업자수
                			   , M.id                                         AS mat_pk        -- 품목아이디
                			   , M."Code"                                     AS mat_code      -- 품목코드
                			   , M."Name"                                     AS mat_name      -- 품목이름
                			   , fn_code_name('mat_type', MG."MaterialType")  AS mat_type      --품목타입
                			   , M."LotSize"                                  AS lot_size      --한 로트당 사이즈
                			   , M."Weight"                                   AS weight        --품목무게
                			   , U."Name"                                     AS unit          --단위이름
                			   , E.id                                         AS equipment_id  --설비 아이디
                			   , E."Name"                                     AS equipment     --설비이름
                			   , C."Description"                              AS description   --설명
                			   , ROUND(C."OrderQty"::numeric, 2)              AS order_qty     --작업 주문수량
                			   , ROUND(C."GoodQty"::numeric, 2)              AS good_qty       --양품수량
                			   , ROUND(C."DefectQty"::numeric, 2)             AS defect_qty    --불량품수량
                			   , C."LossQty"                                  AS loss_qty      --손실수량
                			   , C."ScrapQty"                                 AS scrap_qty     --잔여수량
                			   , TO_CHAR(C."ProductionDate" + M."ValidDays", 'yyyy-mm-dd') AS "ValidDays" -- (생산일+사용기한)
                			   , COALESCE(su."Standard", M."Standard1") as standard            -- 규격
                			   , su."CompanyName" as company_name                              -- 거래처이름
                			   , M."Factory_id" AS "Factory_id"                                -- 공장아이디
                			   , fa."Name" as fac_name                                         -- 공장이름
                			   , S.memo                                                        -- 메모
                               , M."Class1" as class1                                         -- 1차공정
                			   , M."Class2" as class2                                          -- 2차공정
                			   , M."Class3" as class3                                          -- 3차공정
                			   , COALESCE(C."Parent_id") as parent                                        --부모아이디
                			FROM S
                			JOIN job_res       C  ON C.id = S.child_id              -- child = 대표행
                			left join suju su on su.id = C."SourceDataPk" and C."SourceTableName" = 'suju'
                			LEFT JOIN work_center WC ON WC.id = C."WorkCenter_id"
                			LEFT JOIN equ           E  ON E.id  = C."Equipment_id"
                			LEFT JOIN shift         SH ON SH."Code" = C."ShiftCode"
                			LEFT JOIN material      M  ON M.id = C."Material_id"
                			LEFT JOIN mat_grp       MG ON MG.id = M."MaterialGroup_id"
                			LEFT JOIN unit          U  ON U.id = M."Unit_id"
                			left join factory fa on M."Factory_id" = fa.id
                			--WHERE S.rn = 1
                			)
                			SELECT *
                			FROM F
                			WHERE 1=1
                			AND F.mat_name like :matName
                """;

        if(job_proc != null){
            sql += """
                    AND (
                        (:jobProc = 1
                            AND COALESCE(F.class1,'') <> ''
                            AND COALESCE(F.class2,'') = ''
                            AND COALESCE(F.class3,'') = ''
                        )
                     OR (:jobProc = 2
                            AND COALESCE(F.class1,'') <> ''
                            AND COALESCE(F.class2,'') <> ''
                            AND COALESCE(F.class3,'') = ''
                        )
                     OR (:jobProc = 3
                            AND COALESCE(F.class1,'') <> ''
                            AND COALESCE(F.class2,'') <> ''
                            AND COALESCE(F.class3,'') <> ''
                        )
                    )
                    """;
        }

		if ("false".equalsIgnoreCase(isIncludeComp)) {
			// ★ 파생 상태(state) 기준으로 완료 제외
			sql += " and F.state != 'finished' ";
		}

		if (cboFactory != null) {
			sql += " and F.\"Factory_id\" = :cboFactory ";
		}

		sql += " ORDER BY F.prod_date, F.order_num, F.id ";


		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public Map<String, Object> getProdResultDetail(Integer jrPk) {
		MapSqlParameterSource p = new MapSqlParameterSource().addValue("jrPk", jrPk);

        String sql = """
                WITH target AS (
                				SELECT jr.id AS child_id, jr."Parent_id" AS parent_id
                				FROM job_res jr
                				WHERE jr.id = :jrPk
                			),
                			base_pick AS (
                				SELECT COALESCE(parent_id, child_id) AS base_id
                				FROM target
                			)
                			SELECT
                				-- PK들(프런트에서 쓰기 좋게 모두 내려줌)
                				c.id                             AS id,              -- ★ child jr_pk (현재 상세의 주인공)
                				t.parent_id                      AS parent_jr_pk,    -- 부모 있으면 부모 pk
                				b.base_id                        AS base_jr_pk,      -- 부모가 있으면 부모, 없으면 자기 자신
                
                				-- 기본 정보는 base 기준(=부모 우선)
                				c."WorkOrderNumber"              AS order_num,       -- 작업지시번호는 child/parent 동일하므로 child 써도 무방
                				base_m.id                        AS mat_pk,
                				base_m."Code"                    AS mat_code,
                				base_m."Name"                    AS mat_name,
                				base_m."LotSize"                 AS lot_size,
                				u."Name"                         AS unit,
                				ROUND(COALESCE(c."OrderQty", 0)::numeric, 2)   AS order_qty,
                				 ROUND(COALESCE(c."GoodQty", 0)::numeric, 2)    AS good_qty,
                				 ROUND(COALESCE(c."DefectQty", 0)::numeric, 2)  AS defect_qty,
                				 ROUND(COALESCE(c."LossQty", 0)::numeric, 2)    AS loss_qty,
                				 ROUND(COALESCE(c."ScrapQty", 0)::numeric, 2)   AS scrap_qty,
                				to_char(c."ProductionDate",'yyyy-mm-dd') AS prod_date,
                				to_char(c."StartTime",'hh24:mi')         AS start_time,
                				c."EndDate"                               AS end_date,
                				to_char(c."StartTime",'yyyy-mm-dd')       AS start_date,
                				to_char(c."EndTime",'hh24:mi')            AS end_time,
                				c."ShiftCode"                             AS shift_code,
                				sh."Name"                                       AS shift_name,
                				base_m."ValidDays",
                				base_m."Routing_id"                             AS routing_id,
                				base_m."Temperature" as mat_temp,
                				base_m."Pressure" as mat_rpm,
                
                				-- 공정/워크센터/설비/상태는 child 기준(=현재 공정)
                				c."State"                                       AS state,
                				fn_code_name('job_state', c."State")            AS job_state,
                				child_wc.id                                     AS workcenter_id,
                				child_wc."Name"                                 AS workcenter_name,
                				child_wc."Factory_id"                           AS wcfactory_id,
                				e.id                                            AS equipment_id,
                				e."Name"                                        AS equipment_name,
                				child_p.id                                      AS process_id,
                				child_p."Name"                                  AS process_nm,
                
                				-- 필요하면 정렬/표시용
                				c."WorkIndex"                             AS work_idx,
                				c."LotNumber"                                   AS lot_num
                
                			FROM target t
                			JOIN base_pick b                 ON 1=1
                			JOIN job_res c                   ON c.id = t.child_id              -- child
                			--JOIN job_res base_jr             ON base_jr.id = b.base_id         -- base(부모 있으면 부모)
                			LEFT JOIN material base_m        ON base_m.id = c."Material_id"
                			LEFT JOIN unit u                 ON u.id = base_m."Unit_id"
                			LEFT JOIN shift sh               ON sh."Code" = c."ShiftCode"
                			LEFT JOIN work_center child_wc   ON child_wc.id = c."WorkCenter_id"
                			LEFT JOIN process child_p        ON child_p.id = child_wc."Process_id"
                			LEFT JOIN equ e                  ON e.id = c."Equipment_id"
                """;

		return this.sqlRunner.getRow(sql, p);
	}

	public Map<String, Object> getProdResultMatDetail(Integer jrPk) {
		MapSqlParameterSource p = new MapSqlParameterSource().addValue("jrPk", jrPk);

		String sql = """
			select jr.id
			, m."Code" as mat_code
			, m."Name" as mat_name
			, ROUND(jr."OrderQty"::numeric, 2) as "OrderQty"
			, sju."Standard" as standard
			, sju.id as suju_id
			from job_res jr 
			inner join material m on m.id = jr."Material_id" 
			LEFT JOIN suju sju ON sju.id = jr."SourceDataPk"
			where jr.id = :jrPk
			and jr."SourceTableName" ='suju'
			""";

		Map<String, Object> job = this.sqlRunner.getRow(sql, p);
		if (job == null) return null;

		// ② 하위 품목 리스트(suju_detail)
		MapSqlParameterSource p2 = new MapSqlParameterSource().addValue("suju_id", job.get("suju_id"));
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
		List<Map<String, Object>> suju_detail = this.sqlRunner.getRows(sql_suju_detail, p2);

		// ③ items 키로 리스트 추가 (print_report 에서 {%= o.items %})
		job.put("items", suju_detail);

		return job;
	}

	public Map<String, Object> getProdResultPrintDetail(Integer jrPk) {
		MapSqlParameterSource p = new MapSqlParameterSource().addValue("jrPk", jrPk);

		String sql = """
			WITH target AS (
				SELECT jr.id AS child_id, jr."Parent_id" AS parent_id
				FROM job_res jr
				WHERE jr.id = :jrPk
			),
			base_pick AS (
				SELECT COALESCE(parent_id, child_id) AS base_id
				FROM target
			)
			SELECT
				-- PK들(프런트에서 쓰기 좋게 모두 내려줌)
				c.id                             AS id,              -- ★ child jr_pk (현재 상세의 주인공)
				t.parent_id                      AS parent_jr_pk,    -- 부모 있으면 부모 pk
				b.base_id                        AS base_jr_pk,      -- 부모가 있으면 부모, 없으면 자기 자신
		
				-- 기본 정보는 base 기준(=부모 우선)
				c."WorkOrderNumber"              AS order_num,       -- 작업지시번호는 child/parent 동일하므로 child 써도 무방
				base_m.id                        AS mat_pk,
				base_m."Code"                    AS mat_code,
				base_m."Name"                    AS mat_name,
				base_m."LotSize"                 AS lot_size,
				u."Name"  AS unit,
				ROUND(COALESCE(base_jr."OrderQty", 0)::numeric, 2)   AS order_qty,
				 ROUND(COALESCE(base_jr."GoodQty", 0)::numeric, 2)    AS good_qty,
				 ROUND(COALESCE(base_jr."DefectQty", 0)::numeric, 2)  AS defect_qty,
				 ROUND(COALESCE(base_jr."LossQty", 0)::numeric, 2)    AS loss_qty,
				 ROUND(COALESCE(base_jr."ScrapQty", 0)::numeric, 2)   AS scrap_qty,
				to_char(base_jr."ProductionDate",'yyyy-mm-dd') AS prod_date,
				to_char(c."StartTime",'hh24:mi')   AS start_time,
				c."EndDate" AS end_date,
				to_char(c."StartTime",'yyyy-mm-dd') AS start_date,
				to_char(c."EndTime",'hh24:mi')  AS end_time,
				c."ShiftCode"  AS shift_code,
				sh."Name"   AS shift_name,
				base_m."ValidDays",
				base_m."Routing_id"  AS routing_id,
		
				-- 공정/워크센터/설비/상태는 child 기준(=현재 공정)
				c."State"  AS state,
				fn_code_name('job_state', c."State")   AS job_state,
				child_wc.id   AS workcenter_id,
				child_wc."Name"  AS workcenter_name,
				e.id   AS equipment_id,
				e."Name"  AS equipment_name,
				child_p.id AS process_id,
				child_p."Name" AS process_nm,
		
				-- 필요하면 정렬/표시용
				base_jr."WorkIndex" AS work_idx,
				c."LotNumber" AS lot_num,
				base_jr."SourceDataPk" AS suju_id,
				s."CompanyName" as company_name,
				s."Standard" as standard
		
			FROM target t
			JOIN base_pick b                 ON 1=1
			JOIN job_res c                   ON c.id = t.child_id              -- child
			JOIN job_res base_jr             ON base_jr.id = b.base_id         -- base(부모 있으면 부모)
			LEFT JOIN material base_m        ON base_m.id = base_jr."Material_id"
			LEFT JOIN unit u                 ON u.id = base_m."Unit_id"
			LEFT JOIN shift sh               ON sh."Code" = base_jr."ShiftCode"
			LEFT JOIN work_center child_wc   ON child_wc.id = c."WorkCenter_id"
			LEFT JOIN process child_p        ON child_p.id = child_wc."Process_id"
			LEFT JOIN equ e                  ON e.id = c."Equipment_id"
			left join suju s on s.id = base_jr."SourceDataPk" and base_jr."SourceTableName" = 'suju'
			""";

		Map<String, Object> job = this.sqlRunner.getRow(sql, p);
		if (job == null) return null;

		// ② 하위 품목 리스트(suju_detail)
		MapSqlParameterSource p2 = new MapSqlParameterSource().addValue("suju_id", job.get("suju_id"));
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
		List<Map<String, Object>> suju_detail = this.sqlRunner.getRows(sql_suju_detail, p2);

		// ③ items 키로 리스트 추가 (print_report 에서 {%= o.items %})
		job.put("items", suju_detail);

		// ④ title 추가
		job.put("title", "제 품 확 인 서");

		return job;
	}


	public List<Map<String, Object>> getDefectList(Integer jrPk, Integer workcenterId) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);
		dicParam.addValue("workcenterId", workcenterId);

		String sql = """
                 with TOT as (
                          select jrd.id as jrd_id
                          , jrd."DefectQty" as defect_qty
                          , jrd."DefectType_id"  as defect_id
                          , jrd."Description" as defect_remark
                          from job_res_defect jrd 
                          where jrd."JobResponse_id" = :jrPk
                       ), a as(
                         select 
                         jr."WorkCenter_id"
                         , wc."Process_id"
                         , pdt."DefectType_id" as defect_id
                         , dt."Name" as defect_type
                         , coalesce( TOT.defect_qty,0) as defect_qty
                         , TOT.jrd_id
                         , TOT.defect_remark
                         from job_res jr 
                         left join work_center wc on wc.id=jr."WorkCenter_id"  
                         left join proc_defect_type pdt on pdt."Process_id" =wc."Process_id" 
                         inner join defect_type dt on dt.id = pdt."DefectType_id" 
                         left join TOT on TOT.defect_id=dt.id
                         where jr.id = :jrPk
                         )
                         select * from a
                """;

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public List<Map<String, Object>> getChasuList(Integer jrPk) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);

		String sql = """
			 select id
				  , "LotIndex" as chasu
				  , "LotNumber" as lot_no
				  , ROUND("GoodQty"::numeric, 2) as good_qty
				  , ROUND("DefectQty"::numeric, 2) as defect_qty
				  , ROUND("GoodQty"::numeric, 2) as source_good_qty
				  , ROUND("DefectQty"::numeric, 2) as source_defect_qty
				  , "LossQty" as loss_qty
				  , "ScrapQty" as scrap_qty
				  , to_char("EndTime", 'YYYY-MM-DD HH24:MI') as end_time
				  , to_char("StartTime", 'YYYY-MM-DD HH24:MI') as start_time
				  , case
					  when "_modified" is null then to_char("_created", 'YYYY-MM-DD HH24:MI')
					  else to_char("_modified", 'YYYY-MM-DD HH24:MI')
					end as input_time
			 from mat_produce
			 where "JobResponse_id" = :jrPk
			 order by "LotIndex"
			""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

    /**
     * 공정(Job Response)에 투입된 자재의 LOT별 정보와
     * 해당 LOT에서 실제 소비된 수량을 조회한다.
     *
     * @param jrPk     공정 실행(Job Response) PK
     * @param mat_code 자재 코드 (null 또는 빈 값이면 전체 조회)
     * @return LOT 단위의 투입 자재 정보 및 소비 수량 목록
     */
	public List<Map<String, Object>> getInputLotList(Integer jrPk, String mat_code) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);
		dicParam.addValue("mat_code", mat_code);

		String sql = """
                with AA as (
                         select 
                         ml."LotNumber"
                         , sum(mlc."OutputQty") as "OutputQty" 
                         from mat_produce mp 
                         inner join job_res jr on jr.id = mp."JobResponse_id"
                         inner join mat_lot_cons mlc on mlc."SourceDataPk" = mp.id and mlc."SourceTableName" ='mat_produce'   
                         inner join mat_lot ml on ml.id = mlc."MaterialLot_id" 
                         where jr.id= :jrPk group by ml."LotNumber" 
                         ), R as (
                             select  mpir.id as mpir_id
                             , mpi.id as mpi_id
                             , mpi."Material_id" as mat_pk
                             , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
                             , mg."Name" as mat_group_name
                             , m."Code" as mat_code
                             , m."Name" as mat_name 
                             , u."Name" as unit_name
                             , mpi."RequestQty" as req_qty
                             , mpi."InputQty" 
                             , to_char(mpi."InputDateTime",'yyyy-MM-dd') as "InputDateTime"
                             , ml."LotNumber"
                             , ml."CurrentStock" as cur_stock
                             , m."ProcessSafetyStock" as proc_safety_stock
                             , mpi."MaterialStoreHouse_id"
                             , mpi."ProcessStoreHouse_id"
                             , mpi."State"
                             , fn_code_name('mat_proc_input_state', mpi."State") as state_name
                             , sh."Name" as "StoreHouseName"
                             from job_res jr 
                             inner join mat_proc_input_req mpir on mpir.id = jr."MaterialProcessInputRequest_id" 
                             inner join mat_proc_input mpi on mpi."MaterialProcessInputRequest_id" =mpir.id
                             inner join material m on m.id = mpi."Material_id"
                             inner join mat_grp mg on mg.id = m."MaterialGroup_id"
                             left join unit u on u.id = m."Unit_id"
                             left join mat_lot ml on ml.id = mpi."MaterialLot_id"
                             left join store_house sh on sh.id=ml."StoreHouse_id"
                             where jr.id =  :jrPk
                             and (:mat_code is null or :mat_code = '' or m."Code" = :mat_code)
                          )
                          select R.mat_pk, R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
                          , R.mpir_id
                          , R.mpi_id
                          , R.req_qty
                          , R."InputQty" 
                          , R."LotNumber" as lot_number
                          , R.state_name
                          , R.unit_name
                          , R.cur_stock
                          , R."State" 
                          , R."InputDateTime" as start_date
                          , R."StoreHouseName"
                          , COALESCE(AA."OutputQty", 0) as consumed_qty
                          from R 
                          left join AA on AA."LotNumber" = R."LotNumber"
                          order by R."InputDateTime", R."LotNumber"
                	""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public Integer findJobByOrderAndProcess(String orderNum, Integer processId, Integer proMatId) {
		MapSqlParameterSource p = new MapSqlParameterSource()
				.addValue("order_num", orderNum)
				.addValue("process_id", processId)
				.addValue("pro_mat_id", proMatId);
		String sql = """
				SELECT jr.id
				FROM job_res jr
				JOIN work_center wc ON wc.id = jr."WorkCenter_id"
				WHERE jr."WorkOrderNumber" = :order_num
				  AND wc."Process_id" = :process_id
				  AND jr."Material_id" = :pro_mat_id
				ORDER BY jr.id DESC
				LIMIT 1;
				""";
		Map<String,Object> row = sqlRunner.getRow(sql, p);
		return row != null ? (Integer) row.get("id") : null;
	}

    /**
     * 공정 시작 전, 목표 생산 수량을 기준으로
     * BOM에 정의된 자재의 이론적 소요량을 계산한다.
     *
     * @param prodMatId       생산할 제품(Material) ID
     * @param needProMatQty   목표 생산 수량
     * @param prodDate        생산 기준일
     * @return 자재별 이론 소요량 목록 (계획값)
     */
	public List<Map<String, Object>> getConsumedListPlan(Integer prodMatId, BigDecimal needProMatQty, String prodDate) {
		MapSqlParameterSource p = new MapSqlParameterSource();
		p.addValue("prodMatId", prodMatId);
		p.addValue("needQty", needProMatQty);
		p.addValue("prodDate", prodDate);

		String sql = """
        WITH bom1 AS (
            SELECT
                b1.id AS bom_pk,
                b1."Material_id" AS prod_pk,
                b1."OutputAmount" AS produced_qty,
                :needQty::numeric AS order_qty,
                ROW_NUMBER() OVER (PARTITION BY b1."Material_id" ORDER BY b1."Version" DESC) AS g_idx
            FROM bom b1
            WHERE b1."BOMType" = 'manufacturing'
              AND (:prodDate::date IS NULL OR :prodDate::date BETWEEN b1."StartDate" AND b1."EndDate")
              AND b1."Material_id" = :prodMatId
        ),
        BT AS (
            SELECT
                bc."Material_id" AS mat_pk,
                b.produced_qty,
                bc."Amount" AS quantity,
                (bc."Amount" / NULLIF(b.produced_qty,0)) AS bom_ratio,
                (bc."Amount" / NULLIF(b.produced_qty,0)) * b.order_qty AS bom_requ_qty
            FROM bom_comp bc
            JOIN bom1 b ON b.bom_pk = bc."BOM_id"
            WHERE b.g_idx = 1
        )
        SELECT
            BT.mat_pk,
            mg."MaterialType" AS mat_type,
            fn_code_name('mat_type', mg."MaterialType") AS mat_type_name,
            mg."Name" AS mat_group_name,
            m."Code" AS mat_code,
            m."Name" AS mat_name,
            m."LotSize" AS lot_size,
            mh."CurrentStock" AS "currentStock",
            u."Name" AS unit,
            BT.bom_ratio,
            ROUND(BT.bom_requ_qty::numeric) AS bom_consumed,   -- 예상 소요
            0::numeric AS consumed_qty,                        -- 아직 미시작이므로 0
            sh."Name" AS storehouse_name,
            0::numeric AS mc_qty,
            0::numeric AS current_qty_sum,
            COALESCE(m."LotUseYN",'N') AS "lotUseYn",
            CASE WHEN m."Useyn"='1' THEN 'Y' WHEN m."Useyn"='0' THEN 'N' ELSE NULL END AS useyn
        FROM BT
        JOIN material m   ON m.id = BT.mat_pk
        LEFT JOIN mat_grp mg  ON mg.id = m."MaterialGroup_id"
        LEFT JOIN unit u      ON u.id = m."Unit_id"
        LEFT JOIN store_house sh ON sh.id = m."StoreHouse_id"
        LEFT JOIN mat_in_house mh ON mh."Material_id" = m.id AND mh."StoreHouse_id" = m."StoreHouse_id"
        WHERE m."Useyn" = '0'
        ORDER BY m."Code"
    """;

		return this.sqlRunner.getRows(sql, p);
	}


    /**
     * BOM 기준 이론 소요량과
     * 실제 현장에서 발생한 투입/소비 데이터를 비교하기 위한 종합 조회.
     *
     * @param jrPk     공정 실행(Job Response) PK
     * @param prodPk   생산 제품(Material) PK
     * @param prodDate 생산일자
     * @return 자재별 이론 소요량 vs 실제 투입/소비 현황
     */
	public List<Map<String, Object>> getConsumedListFirst(Integer jrPk, Integer prodPk, String prodDate) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);
		dicParam.addValue("prodPk", prodPk);
		dicParam.addValue("prodDate", prodDate);

		String sql = """
                with bom1 as (
						select 
						b1.id as bom_pk
						, b1."Material_id" as prod_pk
						, b1."OutputAmount" as produced_qty
						, jr."OrderQty" as order_qty
						, row_number() over(partition by b1."Material_id" order by b1."Version" desc) as g_idx
						from bom b1
						 inner join job_res jr on jr."Material_id"=b1."Material_id" and jr.id= :jrPk
						where b1."BOMType" = 'manufacturing' and jr."ProductionDate" between b1."StartDate" and b1."EndDate"  
						), BT as (
						select 
						bc."Material_id" as mat_pk
						, round(bom1.produced_qty::numeric, 0) as produced_qty
				    	, round(bc."Amount"::numeric, 0) as quantity
				    	, round((bc."Amount" / bom1.produced_qty)::numeric, 0) as bom_ratio
				    	, round((bc."Amount" / bom1.produced_qty * bom1.order_qty)::numeric, 0) as bom_requ_qty 
						from bom_comp bc 
						inner join bom1 on bom1.bom_pk=bc."BOM_id"
						where bom1.g_idx=1
						), llc as (
						select 
						sum(mlc."OutputQty") as consumed_qty
						, ml."Material_id" 
						from job_res jr 
						inner join mat_produce mp on mp."JobResponse_id" =jr.id and jr.id= :jrPk
						inner join mat_lot_cons mlc on mlc."SourceDataPk" =mp.id and mlc."SourceTableName" ='mat_produce'
						inner join mat_lot ml on ml.id = mlc."MaterialLot_id" 
						group by ml."Material_id" 
						), MCC as (
							select 
							mc."Material_id" as mat_pk
							, sum(mc."ConsumedQty") mc_qty 
							from mat_consu mc 
							where mc."JobResponse_id"= :jrPk group by mc."Material_id"
						), MMP as (
							select 
							sum(ml."OutQtySum") as current_qty_sum
							, mpi."Material_id"
							, sum(round(mpi."RequestQty"::numeric, 0)) as request_qty_sum
							from mat_proc_input mpi
							inner join job_res jr on jr."MaterialProcessInputRequest_id" = mpi."MaterialProcessInputRequest_id" 
							inner join mat_lot ml on ml.id = mpi."MaterialLot_id"
							where jr.id=:jrPk
							group by mpi."Material_id"
						)
						select 
						BT.mat_pk
						, mg."MaterialType" as mat_type
						, fn_code_name('mat_type', mg."MaterialType") as mat_type_name
						, mg."Name" as mat_group_name
						, m."Code" as mat_code
						, m."Name" as mat_name
						, m."LotSize" as lot_size
						, mh."CurrentStock" as "currentStock"
						, u."Name" as unit
						, BT.bom_ratio
						, round(BT.bom_requ_qty::numeric, 4) as bom_consumed
						, COALESCE(llc.consumed_qty,0) as consumed_qty
						, MMP.request_qty_sum
						,round(
				  			(coalesce(round(BT.bom_requ_qty::numeric, 0), 0)   -- = bom_consumed과 동일
				  			- coalesce(round(MMP.request_qty_sum::numeric, 0), 0)
				  			)
						, 4) as remain_input_qty
						, sh."Name" as storehouse_name
						, MCC.mc_qty
						, COALESCE(MMP.current_qty_sum,0) as current_qty_sum
						, coalesce(m."LotUseYN",'N') as "lotUseYn"
						, MMP.request_qty_sum
						,round(
						   (
							 coalesce(round(BT.bom_requ_qty::numeric, 0), 0)   -- = bom_consumed과 동일
						   - coalesce(round(MMP.request_qty_sum::numeric, 0), 0)
						   )
						 , 3) as remain_input_qty
						, CASE WHEN m."Useyn" = '1' THEN 'Y'
							   WHEN m."Useyn" = '0' THEN 'N'
							   ELSE NULL
						  END as useyn
						from BT
						inner join material m on m.id=BT.mat_pk
						left join MCC on MCC.mat_pk=BT.mat_pk
						left join mat_grp mg on mg.id=m."MaterialGroup_id"
						left join unit u on u.id=m."Unit_id"
						left join llc on llc."Material_id" = BT.mat_pk
						left join store_house sh on m."StoreHouse_id" = sh.id
						left join mat_in_house mh on mh."Material_id" = m.id and mh."StoreHouse_id"  = m."StoreHouse_id" 
						left join MMP on MMP."Material_id" = m.id
						where m."Useyn" = '0'
                """;

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

    /**
     * 지정된 공정(process)에 대해,
     * 루트 품목(materialId)의 제조 BOM을 기준으로 하위 BOM을 재귀적으로 탐색하여
     * 공정 산출 품목의 BOM 정보와 누적 소요 비율을 계산한다.
     *
     * <p>계산 결과에는 다음 정보가 포함된다.
     * <ul>
     *   <li>공정 산출 품목 ID 및 품목명</li>
     *   <li>해당 품목의 BOM ID 및 부모 BOM ID</li>
     *   <li>루트 품목 대비 누적 소요 비율(ratio_from_root)</li>
     *   <li>
     *     최상위 주문 수량(orderQty)에 누적 비율을 곱하여 산출한
     *     최종 필요 수량(need_pro_mat_qty)
     *   </li>
     * </ul>
     *
     * <p>재귀 CTE(WITH RECURSIVE)를 사용하여
     * <ol>
     *   <li>유효 기간 및 최신 버전의 제조 BOM을 선택하고</li>
     *   <li>부모 BOM 산출량 대비 자식 BOM 소요량 비율을 누적 계산하며</li>
     *   <li>선택된 공정(processId)에 해당하는 산출 품목만을 집계한다</li>
     * </ol>
     *
     * @param routingId   라우팅 ID (확장용 파라미터)
     * @param processId   조회 대상 공정 ID
     * @param materialId  루트(최상위) 품목 ID
     * @param order_qty   최상위 주문 수량
     * @param prodDate    BOM 유효성 판단 기준 생산일자 (yyyy-MM-dd)
     *
     * @return 공정 산출 품목의 BOM 정보, 누적 비율 및
     *         최종 필요 수량(need_pro_mat_qty)을 포함한 결과 Map
     */
	public Map<String, Object> getProcessStepMeta(
			Integer routingId, Integer processId, Integer materialId, BigDecimal order_qty, String prodDate) {

		MapSqlParameterSource p = new MapSqlParameterSource();
		p.addValue("routingId", routingId);
		p.addValue("processId", processId);
		p.addValue("materialId", materialId);
		p.addValue("orderQty", order_qty);
		p.addValue("prodDate", prodDate);

		String sql = """
			-- inputs: :materialId(루트 품목), :processId, :prodDate, :orderQty [, :routingId]
			 WITH RECURSIVE walk AS (
			   -- 루트의 유효/최신 제조 BOM
			   WITH root_bom AS (
				 SELECT b1.id AS bom_pk,
						b1."Material_id"          AS node_mat_id,
						b1."OutputAmount"::numeric AS node_out,        -- ★ numeric 고정
						ROW_NUMBER() OVER (PARTITION BY b1."Material_id" ORDER BY b1."Version" DESC) AS rn  --같은 Material_id 별로 Version이 높은 순서대로 1, 2, 3… 번호를 매긴다
				 FROM bom b1
				 WHERE b1."BOMType" = 'manufacturing'
				   AND :prodDate::date BETWEEN b1."StartDate" AND b1."EndDate"
				   AND b1."Material_id" = :materialId
			   )
			   SELECT rb.bom_pk,
					  rb.node_mat_id,
					  rb.node_out,                                     -- ★ numeric
					  NULL::integer AS parent_bom_pk,
					  NULL::integer AS parent_mat_pk,
					  1 AS lvl,
					  1::numeric AS cum_ratio                          -- ★ numeric로 시작
			   FROM root_bom rb
			   WHERE rb.rn = 1
			 
			   UNION ALL
			 
			   -- 하위 확장: (자식 소요 / 부모 산출) 비율 누적
			   SELECT child.bom_pk,
					  child.mat_id           AS node_mat_id,
					  child.out_amt::numeric AS node_out,              -- ★ numeric
					  w.bom_pk               AS parent_bom_pk,
					  w.node_mat_id          AS parent_mat_pk,
					  w.lvl + 1              AS lvl,
					  ( w.cum_ratio
						* ( bc."Amount"::numeric / NULLIF(w.node_out,0)::numeric )
					  )::numeric AS cum_ratio                          -- ★ 재귀식도 numeric
			   FROM walk w
			   JOIN bom_comp bc ON bc."BOM_id" = w.bom_pk              -- bom 상세
				 
			   JOIN LATERAL (
				 SELECT b2.id AS bom_pk,
						b2."Material_id" AS mat_id,
						b2."OutputAmount"::numeric AS out_amt          -- ★ numeric
				 FROM bom b2
				 WHERE b2."BOMType" = 'manufacturing'
				   AND :prodDate::date BETWEEN b2."StartDate" AND b2."EndDate"
				   AND b2."Material_id" = bc."Material_id"
				 ORDER BY b2."Version" DESC
				 LIMIT 1
			   ) child ON TRUE
			 ),
			 
			 targets AS (  -- 선택 공정에 해당하는 산출품 후보
			   SELECT
				 w.node_mat_id                     AS pro_mat_id,
				 MIN(w.bom_pk)                     AS bom_id,
				 MIN(w.parent_bom_pk)              AS parent_bom_id,
				 MIN(w.lvl)                        AS lvl,
				 SUM(w.cum_ratio)::numeric         AS ratio_from_root  -- ★ numeric
			   FROM walk w
			   JOIN material m     ON m.id  = w.node_mat_id
			   JOIN work_center wc ON wc.id = m."WorkCenter_id"
			   WHERE wc."Process_id" = :processId
			   GROUP BY w.node_mat_id
			 )
			 
			 SELECT
			   t.pro_mat_id,
			   m."Name" AS pro_mat_nm,
			   t.bom_id,
			   t.parent_bom_id,
			   t.ratio_from_root,
			   ( :orderQty::numeric * COALESCE(t.ratio_from_root,0) )::numeric AS need_pro_mat_qty  -- ★ 최상위 지시량 적용
			 FROM targets t
			 LEFT JOIN material m ON m.id = t.pro_mat_id
			 ORDER BY t.lvl;
	  """;

		return this.sqlRunner.getRow(sql, p);
	}

	public List<Map<String, Object>> getConsumedByProcess(
			Integer routingId, Integer processId, Integer materialId, BigDecimal order_qty, String prodDate) {

		MapSqlParameterSource p = new MapSqlParameterSource();
		p.addValue("routingId", routingId);
		p.addValue("processId", processId);
		p.addValue("materialId", materialId);
		p.addValue("orderQty", order_qty);
		p.addValue("prodDate", prodDate);

		String sql = """
				WITH bd AS (
						SELECT * FROM tbl_bom_detail(:materialId::varchar, :prodDate)
					  ),
					  root AS (SELECT DISTINCT prod_pk FROM bd),
					  sfg_by_parent AS (
						SELECT DISTINCT bd.parent_mat_pk AS sfg_mat_pk
						FROM bd
						JOIN material pm   ON pm.id = bd.parent_mat_pk
						JOIN work_center wc ON wc.id = pm."WorkCenter_id"
						WHERE bd.parent_mat_pk IS NOT NULL
						  AND wc."Process_id" = :processId
					  ),
					  sfg_by_root AS (
						SELECT r.prod_pk AS sfg_mat_pk
						FROM root r
						JOIN material rm   ON rm.id = r.prod_pk
						JOIN work_center wc ON wc.id = rm."WorkCenter_id"
						WHERE wc."Process_id" = :processId
					  ),
					  sfg AS (SELECT sfg_mat_pk FROM sfg_by_parent UNION SELECT sfg_mat_pk FROM sfg_by_root),
					  
					  -- 필요자재(직계)
					  components AS (
						SELECT bd.*
						FROM bd
						JOIN sfg s ON
							 bd.parent_mat_pk = s.sfg_mat_pk
							 OR (bd.parent_mat_pk IS NULL AND bd.prod_pk = s.sfg_mat_pk) -- 루트 공정
					  )
					  SELECT
						(SELECT MIN(bd2.bom_pk) FROM bd bd2 WHERE bd2.parent_mat_pk = c.parent_mat_pk) AS bom_id,
						(SELECT MIN(bd3.parent_bom_pk) FROM bd bd3 WHERE bd3.mat_pk = c.parent_mat_pk) AS parent_bom_id,
						c.parent_mat_pk                             AS pro_mat_id,
						c.mat_pk                                    AS component_id,
						m."Code"                                    AS component_code,
						m."Name"                                    AS component_name,
						u."Name"                                    AS unit,
						c.bom_ratio                                 AS bom_ratio_from_root,
						ROUND((c.bom_ratio * :orderQty)::numeric)   AS need_qty
					  FROM components c
					  JOIN material m ON m.id = c.mat_pk
					  LEFT JOIN unit u ON u.id = m."Unit_id"
					  WHERE m."Useyn" = '0'
					  ORDER BY m."Code";
					  
	  """;

		return this.sqlRunner.getRows(sql, p);
	}

	public List<Map<String, Object>> getConsumedByRoutingProcess(
			Integer routingId, Integer processId, Integer materialId, BigDecimal order_qty, String prodDate) {

		MapSqlParameterSource p = new MapSqlParameterSource();
		p.addValue("routingId", routingId);
		p.addValue("processId", processId);
		p.addValue("materialId", materialId);
		p.addValue("orderQty", order_qty);
		p.addValue("prodDate", prodDate);

		String sql = """
				WITH bd AS (
						SELECT * FROM tbl_bom_detail(:materialId::varchar, :prodDate)
					  ),
					  root AS (SELECT DISTINCT prod_pk FROM bd),
					  sfg_by_parent AS (
						SELECT DISTINCT bd.parent_mat_pk AS sfg_mat_pk
						FROM bd
						JOIN material pm   ON pm.id = bd.parent_mat_pk
						JOIN work_center wc ON wc.id = pm."WorkCenter_id"
						WHERE bd.parent_mat_pk IS NOT NULL
						  AND wc."Process_id" = :processId
					  ),
					  sfg_by_root AS (
						SELECT r.prod_pk AS sfg_mat_pk
						FROM root r
						JOIN material rm   ON rm.id = r.prod_pk
						JOIN work_center wc ON wc.id = rm."WorkCenter_id"
						WHERE wc."Process_id" = :processId
					  ),
					  sfg AS (SELECT sfg_mat_pk FROM sfg_by_parent UNION SELECT sfg_mat_pk FROM sfg_by_root),
					  
					  -- 필요자재(직계)
					  components AS (
						SELECT bd.*
						FROM bd
						JOIN sfg s ON
							 bd.parent_mat_pk = s.sfg_mat_pk
							 OR (bd.parent_mat_pk IS NULL AND bd.prod_pk = s.sfg_mat_pk) -- 루트 공정
					  )
					  SELECT
						(SELECT MIN(bd2.bom_pk) FROM bd bd2 WHERE bd2.parent_mat_pk = c.parent_mat_pk) AS bom_id,
						(SELECT MIN(bd3.parent_bom_pk) FROM bd bd3 WHERE bd3.mat_pk = c.parent_mat_pk) AS parent_bom_id,
						c.parent_mat_pk                             AS pro_mat_id,
						c.mat_pk                                    AS component_id,
						m."Code"                                    AS component_code,
						m."Name"                                    AS component_name,
						u."Name"                                    AS unit,
						c.bom_ratio                                 AS bom_ratio_from_root,
						ROUND((c.bom_ratio * :orderQty)::numeric)   AS need_qty
					  FROM components c
					  JOIN material m ON m.id = c.mat_pk
					  LEFT JOIN unit u ON u.id = m."Unit_id"
					  WHERE m."Useyn" = '0'
					  ORDER BY m."Code";
					  
	  """;

		return this.sqlRunner.getRows(sql, p);
	}


	public List<Map<String, Object>> getConsumedListSecond(Integer jrPk, Integer prodPk, String prodDate) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);
		dicParam.addValue("prodPk", prodPk);
		dicParam.addValue("prodDate", prodDate);

		String sql = """
                with A as (
                                select 
                                l."Material_id" as mat_id
                                , sum(lc."OutputQty") as lot_consumed
                                from job_res jr
                                inner join mat_produce mp on mp."JobResponse_id" = jr.id 
                                inner join mat_lot_cons lc on lc."SourceDataPk" = mp.id
                                inner join mat_lot l on l.id = lc."MaterialLot_id" 
                                where lc."SourceTableName" = 'mat_produce'
                                and jr.id = :jrPk
                                group by l."Material_id"
                            )
                            select m.id as mat_pk
                            , m."Name" as mat_name
                            , u."Name" as unit
                            , fn_unit_ceiling( bom.bom_ratio * , u."PieceYN" ) as bom_consumed
                            , A.lot_consumed
                            , A.lot_consumed as consumed
                            from tbl_bom_detail(cast(:prodPk as text), cast(to_char(cast(:prodDate as date),'YYYY-MM-DD') as text)) as bom
                            inner join material m on m.id = bom.mat_pk
                            left join unit u on u.id = m."Unit_id"
                            left join A on A.mat_id = m.id
                            where bom.b_level = 1
                            order by tot_order 
                """;

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public List<Map<String, Object>> prodTestList(Integer jrPk, Integer testResultId) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("jrPk", jrPk);
		param.addValue("testResultId", testResultId);

		String sql = """
                	select ti.id, up."Name" as "CheckName", ti."ResultType" as "resultType"
                	, tim."SpecText" as "specText"
                	, to_char(tir."TestDateTime", 'YYYY-MM-DD') as "testDate"
                	, tir."JudgeCode", tir."InputResult" as "ctRemark" ,tir."CharResult" as "ntRemark" , ti."Name" as name 
                	, tir."Char1" as result1, tir."Char2" as result2
                	, tr.id as "testResultId", tr."TestMaster_id" as "testMasterId"
                	from test_item_result tir
                	inner join test_result tr on tr.id = tir."TestResult_id"
                	inner join test_mast tm on tm.id = tr."TestMaster_id" 
                	inner join test_item ti on tir."TestItem_id"  = ti.id 
                	inner join test_item_mast tim on ti.id = tim."TestItem_id" and tim."TestMaster_id" = tm.id
                	inner join user_profile up on tir."_creater_id"  = up."User_id" 
                	where tr."SourceTableName" = 'job_res' and tr."SourceDataPk" = :jrPk
                	and tr.id = :testResultId
                	order by ti.id
                """;

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

		return items;
	}

	public List<Map<String, Object>> prodTestDefaultList() {

		String sql = """
                select ti.id, ti."Name" as name , ti."ResultType" as "resultType", tim."SpecText" as "specText", '' as result1, '' as result2 
                from test_item_mast tim 
                inner join test_mast tm on tim."TestMaster_id"  = tm.id 
                inner join test_item ti on tim."TestItem_id"  = ti.id
                where tm."Name"  = '제품검사'
                   """;

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);

		return items;
	}

	public Integer getTestMasterByItem(Integer jrPk) {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("jrPk", jrPk);

		String sql = """
                    SELECT tmm."TestMaster_id" AS testMasterId
                            FROM job_res jr
                            INNER JOIN test_mast_mat tmm ON jr."Material_id" = tmm."Material_id"
                            WHERE jr.id = :jrPk
                            LIMIT 1
                """;

		List<Map<String, Object>> result = this.sqlRunner.getRows(sql, param);
		return result.isEmpty() ? null : (Integer) result.get(0).get("testMasterId");
	}


	public List<Map<String, Object>> prodTestListByTestMaster(Integer testMasterId) {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("testMasterId", testMasterId);

		String sql = """
                    SELECT tm.id AS testMasterId, ti.id, ti."Name" AS name, ti."ResultType" AS "resultType",
                           tim."SpecText" AS "specText", '' AS result1, '' AS result2
                    FROM test_item_mast tim
                    INNER JOIN test_mast tm ON tim."TestMaster_id" = tm.id
                    INNER JOIN test_item ti ON tim."TestItem_id" = ti.id
                    WHERE tm.id = :testMasterId
                """;

		return this.sqlRunner.getRows(sql, param);
	}




	public Map<String, Object> getJobResponseGoodDefectQty(Integer jrPk) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("jrPk", jrPk);

		String sql = """
                select jr.id
                	  ,coalesce(sum(mp."GoodQty"),0) as good_qty
                	  ,coalesce(sum(mp."DefectQty"),0) as defect_qty
                from job_res jr
                inner join mat_produce mp on mp."JobResponse_id" = jr.id
                where jr.id = :jrPk
                group by jr.id
                """;

		Map<String, Object> items = this.sqlRunner.getRow(sql, param);

		return items;
	}

	public float getChasuDefectQty(Integer jrPk) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("jrPk", jrPk);

		String sql = """
                select coalesce(sum(mp."DefectQty"),0) as defect_qty 
                from mat_produce mp 
                   			where mp."JobResponse_id" = :jrPk
                   		""";

		Map<String, Object> items = this.sqlRunner.getRow(sql, param);

		float qty = Float.parseFloat(items.get("defect_qty").toString());

		return qty;
	}

    //SRP에 위배됨. 근데 그냥 검증 + 부수 로직 한군데 모은것
    public Timestamp workFinish_validation(JobRes jr, Map<String, Object> dateParam, List<MaterialConsume> mcList, List<MaterialProduce> mp){

        String endDate = UtilClass.getStringSafe(dateParam.get("endDate"));
        String endTime = UtilClass.getStringSafe(dateParam.get("endTime"));
        String prodDate = UtilClass.getStringSafe(dateParam.get("prodDate"));
        String startTime = UtilClass.getStringSafe(dateParam.get("startTime"));

        validator.validateJobResExists(jr, "작업지를 찾을 수 없습니다."); //작업지시 존재하는지 체크
        validator.workFinish_Exists_endDate(endDate, endTime, "종료일/종료시간이 필요합니다."); //종료일자 및 종료시간 있는지 체크

        DateTimeFormatter dtm = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

        int sec = 0;
        if(jr.getStartTime() != null){
            sec = jr.getStartTime().toLocalDateTime().getSecond();
        }

        // 3) end_time = end_date + end_time (+ sec)

        LocalDateTime endDt = LocalDateTime.parse(endDate + " " + endTime, dtm).withSecond(sec);
        Timestamp end_time = Timestamp.valueOf(endDt);

        // 4) startDt : DB값 우선. (없다면 form 값으로 보정, 그래도 없으면 오류)
        LocalDateTime startDt = null;
        if(jr.getStartTime() != null){
            startDt = jr.getStartTime().toLocalDateTime();
        } else if(prodDate != null && !prodDate.isBlank() && startTime != null && !startTime.isBlank()){
            startDt = LocalDateTime.parse(prodDate + " " + startTime, dtm).withSecond(sec);
        } else {
            throw new CustomException("시작시간이 없습니다. (작업시작 후 완료해주세요)");
        }

        // 5) 백엔드에서도 시간 역전 검증
        validator.reverseTimeValidator(endDt, startDt, "작업시간이 잘못되었습니다. (종료 < 시작)");

        // 6) 생산/차수/투입 체크
        validator.assertNotEmpty(mcList, "저장된 투입내역이 없습니다.\n투입내역을 저장해주세요.");

        validator.assertNotEmpty(mp, "저장된 차수내역이 없습니다.\n차수내역을 저장해주세요.");

        return end_time;
    }


    /// 작업지시 삭제   ///  /////////////////////////////////////////////////////////////////
    //region : 작업지시 삭제 로직
    @Transactional
    public void deleteJobRes(JobRes jr, Integer equipmentId, String orderNum, User user){
        validator.validateJobResExists(jr, "대상 작업지시를 찾을 수 없습니다.");

        boolean isChild = jr.getParentId() != null;

        if(isChild){
            deleteChildJobRes(jr, equipmentId, orderNum, user);
        }else{
            deleteParentJobRes(jr, equipmentId, orderNum, user);
        }

        jobResProcessTreeRepository.deleteByWorkOrderNo(jr.getWorkOrderNumber());
    }

    private void deleteChildJobRes(JobRes jr, Integer equipmentId, String orderNum, User user){

        if(equipmentId == null) throw new CustomException("장비 정보가 없어 삭제할 수 없습니다.");

        prodResultDelValidator(jr,
                "대상 작업지시를 찾을 수 없습니다.",
                "생산량이 존재하여 삭제할 수 없습니다.",
                "등록된 차수가 있어 삭제할 수 없습니다.");

        //TODO: 삭제하면 이전 공정의 parentId 가르키는 것을 바꿔줘야 한다.
        BomTreeService treeService = new BomTreeService();

        int deleted = equRunRepository.deleteByWorkOrderNumberAndEquipmentId(orderNum, equipmentId);

        // 자식 JobRes 삭제
        jobResRepository.deleteById(jr.getId());

    }

    private void deleteParentJobRes(JobRes jr, Integer equipmentId, String orderNum, User user){
        // ======================
        // PLAN(부모) 취소 플로우
        // ======================
        // 진행 중인 설비가동은 stop 처리 (기존 로직 유지)
        Timestamp now = DateUtil.getNowTimeStamp();

        equRunRepository.findLatestRunningByEquipmentAndOrder(equipmentId, orderNum)
                .ifPresent(run -> {
                    if (run.getEndDate() == null) {
                        run.setEndDate(now);
                        run.setRunState("stop");
                    }
                    run.setDescription("작지 취소");
                    run.set_audit(user);
                    equRunRepository.save(run);
                });

        // 부모 상태만 canceled 로 업데이트 (이력 보존)
//            jr.setState("canceled");
//            jr.set_audit(user);
//            jobResRepository.save(jr);

        //자식 중에 일하고있는애 있는지 확인
        List<JobRes> child_list = jobResRepository.findBySourceDataPkAndSourceTableName(jr.getSourceDataPk(), jr.getSourceTableName());
        for (JobRes child : child_list) {
            if (child.getState().equals("working")) throw new CustomException("관련 공정 중에 진행중인 공정이 존재합니다.");

            //타당성 및 정합성 체크
            prodResultDelValidator(child, "대상 작업지시를 찾을 수 없습니다."
                    , "관련 공정 중에 등록된 차수가 존재합니다.", "관련 공정 중에 생산량이 존재하여 삭제할 수 없습니다.");
        }

        //자식&부모 작지 삭제
        jobResRepository.deleteAll(child_list);
    }

    private void prodResultDelValidator(JobRes jr, String errMsg, String errMsg2, String errMsg3){


        validator.validateJobResExists(jr, errMsg);

        // 1) 생산량 가드: 양품+불량 > 0 이면 취소/삭제 불가
        double good = jr.getGoodQty() == null ? 0d : jr.getGoodQty().doubleValue();
        double defect = jr.getDefectQty() == null ? 0d : jr.getDefectQty().doubleValue();

        if (good + defect > 0) {
            throw new CustomException(errMsg2);
        }

        // (선택) 차수/투입 등 존재 시 가드 유지
        // 기존 코드 유지: 등록된 차수(=소모/투입 등) 있으면 삭제 불가
        List<MaterialConsume> mcList = matConsuRepository.findByJobResponseId(jr.getId());
        if (mcList != null && !mcList.isEmpty()) {
            throw new CustomException(errMsg3);
        }
    }
    //endregion : 작업지시 삭제로직 끝
    ///  끝 /////////////////////////////////////////////////////////////////


    // 다음 공정 알아내기 (부모 JsonNode 반환)
    public BomNode completeAndGetParentNode(
            Map<String, BomNode> processTree,
            int targetMatPk
    ) {

        //이전 정보 제거 (각 노드들의 current 정보를 다 false로 리셋)
        processTree.values().forEach(BomNode::clearCursor);

        for (BomNode root : processTree.values()) {
            BomNode parent = completeAndFindParent(root, null, targetMatPk);
            if (parent != null) {
                return parent;
            }
        }
        return null; // 최상위 공정
    }

    private BomNode completeAndFindParent(
            BomNode current,
            BomNode parent,
            int targetMatPk
    ) {

        if (current.matPk != null && current.matPk == targetMatPk) {
            current.complete = true; //완료 처리

            current.current = true; //현재 공정 표시


            return parent; // 다음 공정
        }

        for (BomNode child : current.children) {
            BomNode found =
                    completeAndFindParent(child, current, targetMatPk);
            if (found != null) {
                return found;
            }
        }

        return null;
    }


    public void saveNextOfProcess(JobRes previous, BomNode node, User user) {

        Optional<JobRes> jr = jobResRepository.findTopByWorkOrderNumberOrderByWorkIndexDesc(previous.getWorkOrderNumber());

        if(!jr.isPresent()) throw new CustomException("작업지시를 찾을 수 없습니다.");

        int workIndex = jr.get().getWorkIndex() + 1;

        JobRes next = new JobRes();
        next.set_audit(user); //공통
        next.setProductionPlanDate(previous.getProductionPlanDate());
        next.setProductionDate(previous.getProductionDate());
        next.setShiftCode(previous.getShiftCode());
        next.setWorkIndex(workIndex);
        next.setOrderQty(node.calculatedBomRatio.floatValue());
        next.setState("ordered");
        next.setParentId(previous.getParentId());
        next.setProcessCount(previous.getProcessCount()+1);
        next.setSourceDataPk(previous.getSourceDataPk());
        next.setSourceTableName(previous.getSourceTableName());
        next.setFirstWorkCenter_id(previous.getFirstWorkCenter_id());

        next.setMaterialId(node.matPk);

        next.setStoreHouse_id(node.storeHouseId);

        next.setWorkCenter_id(previous.getWorkCenter_id());
        next.setSpjangcd(previous.getSpjangcd());

        jobResRepository.save(next);

        //: 이전공정이 물고있는 parentId 수정해줘여함
        previous.setParentId(next.getId());
        jobResRepository.save(previous);

    }

    /// chasu_add
    //endregion 차수별 생산 api
    public AjaxResult chasu_add_service(Integer jrPk, Float goodQty, String spjangcd, User user){

        AjaxResult result = new AjaxResult();
        Timestamp now = DateUtil.getNowTimeStamp();

        // 현재 일자
        LocalDate date = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // 현재 시간
        LocalTime time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss");

        JobRes jr = this.jobResRepository.getJobResById(jrPk);
        validator.throwIfNull(jr, "작업지시가 존재하지 않습니다.");
        validator.throwIfNull(jr.getWorkCenter_id(), "워크센터가 지정되지 않았습니다.");

        Material m = this.materialRepository.getMaterialById(jr.getMaterialId());

        validator.throwIfNull(m, "해당 작업지시에 대한 품목정보가 없습니다.");
        validator.throwIfNull(m.getStoreHouseId(), "생산제품의 기본 창고가 설정되어 있지 않습니다.");

        Integer storehouseId = m.getStoreHouseId();

        // 작업지시량 / lot당 수량 (올림해야댐) ==> 생성해야할 LOT 수
        int totalLotCnt = (int) Math.ceil(jr.getOrderQty() / goodQty);

        //각 LOT별 수량
        int orderQty =  jr.getOrderQty().intValue();
        int lotQty = goodQty.intValue();
        List<Integer> lotQtyList = IntStream.rangeClosed(1, totalLotCnt)
                .map(i -> i < totalLotCnt ? lotQty : orderQty - lotQty * (totalLotCnt - 1)).boxed().toList();


        // matprods 개수로
        List<MaterialProduce> mpList = this.matProduceRepository.findByJobResponseId(jr.getId());
        Integer startChasu = mpList.size() + 1;
        List<Integer> lotChasuList = IntStream.rangeClosed(startChasu, startChasu + totalLotCnt).boxed().toList();


        // lot_size = material.LotSize
        Workcenter wc = this.workcenterRepository.getWorkcenterById(jr.getWorkCenter_id());
        Integer processId = wc.getProcessId();

        // 1. 로트번호 생성
        // lot 자동 생성
        String lotPrefix = "B";

        MaterialGroup mg = this.materialGroupRepository.getMatGrpById(m.getMaterialGroupId());
        if (mg.getMaterialType().equals("product")) {
            lotPrefix = "P";
        }

        //String lotNumber = this.lotService.make_production_lot_in_number(lotPrefix);
        List<String> lotNumberList = this.lotService.make_production_lotList_in_number(lotPrefix, totalLotCnt);

        // 차수별 mat_produce
        List<MaterialProduce> mpEntityList = new ArrayList<>();
        List<MaterialLot> mlEntityList = new ArrayList<>();

        for(int i=0; i < totalLotCnt; i++) {
            MaterialProduce mp = new MaterialProduce();
            mp.setJobResponseId(jr.getId());
            mp.setMaterialId(m.getId());
            mp.setProcessId(processId);
            mp.setProcessOrder(1);
            mp.setLotIndex(lotChasuList.get(i));
            mp.setState("finished");
            mp.set_status("a");
            mp.setStoreHouseId(storehouseId);
            mp.setProductionDate(jr.getProductionDate());
            mp.setStartTime(jr.getStartTime());
            mp.setEndTime(now);
            mp.setShiftCode(jr.getShiftCode());
            mp.setWorkCenterId(jr.getWorkCenter_id());
            mp.setEquipmentId(jr.getEquipment_id());
            mp.setGoodQty(lotQtyList.get(i).floatValue());
            mp.setDescription("차수생산");
            mp.setActorId(user.getId());
            mp.set_audit(user);
            mp.setLastProcessYN("Y");
            mp.setLotNumber(lotNumberList.get(i));
            mp.setSpjangcd(spjangcd);
            mpEntityList.add(mp);
            this.matProduceRepository.save(mp);

            MaterialLot ml = new MaterialLot();
            ml.setLotNumber(lotNumberList.get(i));
            ml.setMaterialId(m.getId());
            ml.setInputDateTime(now);
            ml.setInputQty(mp.getGoodQty());
            ml.setCurrentStock(mp.getGoodQty());
            ml.setDescription(lotChasuList.get(i) + "차수생산");
            ml.setSourceDataPk(mp.getId());
            ml.setSourceTableName("mat_produce");
            ml.setStoreHouseId(mp.getStoreHouseId());
            ml.set_audit(user);
            ml.setSpjangcd(spjangcd);
            mlEntityList.add(ml);

            this.matLotRepository.save(ml);
        }

        // 차수생산량 만큼 good_qty량 만큼 BOM 수량조회
        List<Map<String, Object>> bomMatItems = this.get_chasu_bom_mat_qty_list(mpEntityList.get(0).getId());
        validator.assertNotEmpty(bomMatItems, "BOM구성이 없습니다.");


        for (int i = 0; i < bomMatItems.size(); i++) {
            Map<String, Object> bomMap = bomMatItems.get(i);

            float chasuBomQty = Float.parseFloat(bomMap.get("chasu_bom_qty").toString());
            Float bom_ratio   = bomMap.get("bom_ratio") == null ? null : Float.parseFloat(bomMap.get("bom_ratio").toString());
            if(bom_ratio == null) throw new CustomException("bom 수량이 존재하지 않습니다.");

            int consumeMatPk = (int) bomMap.get("mat_pk");
            String matName = bomMap.get("mat_name").toString();
            Material consMat = this.materialRepository.getMaterialById(consumeMatPk);
            String lotUseYn = bomMap.get("lotUseYn").toString();
            float totalQty = 0f;

            /*
			 선입선출로 mat_lot 찾아서 차감
             차감하면서 mat_lot_cons 생성
             투입되어야할 수량보다 적으면 재고량 부족으로 return
             */

            if ("Y".equals(lotUseYn)) { //lot 관리를 할 경우 //TODO
                // 수정시작
                // 1. mat_proc_input 에서 해당 품목의 로트리스트를 가져온다.

                List<Map<String, Object>> mpiList = this.getMaterialProcessInputList(jr.getId(), consumeMatPk);
                // 투입요청에서 해당 품목이 로트 투입인지 조회한다

                float remainQty = chasuBomQty; //남은 투입량

                /*for (int j = 0; j < mpiList.size(); j++) {
                    Map<String, Object> mpiMap = mpiList.get(j);

                    float reqQty = Float.parseFloat(mpiMap.get("req_qty").toString());
                    totalQty += reqQty;

                    int matLotId = (int) mpiMap.get("ml_id");
                    float currentStock = Float.parseFloat(mpiMap.get("curr_qty").toString());
                    if (currentStock == 0) { //TODO: 수량이 부족한데 예외 안터지고 그냥 반복문만 빠져나감... 뭐지?
                        continue;
                    }

                    MatLotCons mlc = new MatLotCons();
                    mlc.setMaterialLotId(matLotId);
                    mlc.setOutputDateTime(now);
                    mlc.setSourceDataPk(mp.getId());
                    mlc.setSourceTableName("mat_produce");
                    mlc.set_audit(user);
                    mlc.setCurrentStock(ml.getCurrentStock()); // 당시 재고량
                    mlc.setSpjangcd(spjangcd);
                    if (currentStock >= remainQty) {
                        // 해당로트의현재수량 가능
                        mlc.setOutputQty(reqQty);
                        remainQty = (float) 0;
                        mlc = this.matLotConsRepository.save(mlc);

                        break;
                    } else {
                        mlc.setOutputQty(reqQty);
                        mlc = this.matLotConsRepository.save(mlc);
                        remainQty = remainQty - reqQty;
                    }

                }*/

//                if (remainQty > 0) {
//                    result.message = "로트 수량이 부족합니다.(" + matName + ")";
//                    result.success = false;
//                    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
//                    return result;
//                }
            } else {
                if ("1".equals(consMat.getUseyn())) {
                    throw new CustomException("사용 불가능한 품목이 BOM에 등록되어 있습니다.(" + matName + ")");
                }
                // mtyn이 0일 때는 재고 체크하지 않음
                if ("0".equals(consMat.getMtyn())) {
                    // 아무 동작 안함.
                } else {
                    Float currentStock = consMat.getCurrentStock();
                    if (currentStock == null || currentStock == 0f) {
                        throw new CustomException("가용한 품목 재고가 없습니다.(" + matName + ")");
                    } else if (currentStock < goodQty) {
                        throw new CustomException("가용한 품목 재고가 부족합니다. \n(" +
                                matName + ", 필요 수량: " + goodQty + ", 가용 수량: " + currentStock + ")");
                    }
                }
                totalQty += chasuBomQty; //TODO 아래보니깐 그냥 chasuBomQty가 맞는듯함.
            }

            for(int j=0; j<mpEntityList.size(); j++){
                // mat_cons 생성
                MaterialConsume mc = new MaterialConsume();
                mc.setJobResponseId(jr.getId());
                mc.setMaterialId(consumeMatPk);
                mc.setProcessOrder(1);
                mc.setLotIndex(mpEntityList.get(j).getLotIndex());
                mc.setStartTime(now);
                mc.setEndTime(now);
                mc.setDescription("차수생산분");
                mc.setBomQty(chasuBomQty); //TODO

                Float goodQty1 = mpEntityList.get(j).getGoodQty();

                mc.setConsumedQty(goodQty1 * bom_ratio);        // 차수 생산분에 해당하는 BOM기준물량, lot 마다 투입 수량  //TODO
                mc.set_audit(user);
                mc.setState("finished");
                mc.set_status("a");
                mc.setStoreHouseId(consMat.getStoreHouseId());
                mc.setSpjangcd(spjangcd);
                mc = this.matConsuRepository.save(mc);

                //1. mat_inout 생성=> lot 투입이면 투입 수량만큼 lot 없으면 BOM 수량만큼 재고를 차감한다.
                MaterialInout mic = new MaterialInout();
                mic.setMaterialInoutHeadId(null);
                mic.setMaterialId(mc.getMaterialId());
                mic.setStoreHouseId(consMat.getStoreHouseId());
                mic.setLotNumber(mpEntityList.get(j).getLotNumber());
                mic.setInoutDate(LocalDate.parse(date.format(dateFormat)));
                mic.setInoutTime(LocalTime.parse(time.format(timeFormat)));
                mic.setInOut("out");
                mic.setOutputType("consumed_out");
                mic.setOutputQty(mc.getConsumedQty());
                mic.setSourceDataPk(mc.getId());
                mic.setSourceTableName("mat_consu");
                mic.setState("confirmed");
                mic.set_status("a");
                mic.setDescription("차수생산 투입재고 차감");
                mic.set_audit(user);
                mic.setSpjangcd(spjangcd);

                this.matInoutRepository.save(mic);
            }

        } // bom List 반목문 끝, for문 끝

        // 2. mat_inout 생성=> 차수 수량만큼 재고를 증감한다.

        for(int i=0; i<totalLotCnt; i++){
            MaterialInout mip = new MaterialInout();
            mip.setMaterialInoutHeadId(null);
            mip.setMaterialId(m.getId());
            mip.setStoreHouseId(m.getStoreHouseId());
            mip.setLotNumber(lotNumberList.get(i));
            mip.setInoutDate(LocalDate.parse(date.format(dateFormat)));
            mip.setInoutTime(LocalTime.parse(time.format(timeFormat)));
            mip.setInOut("in");
            mip.setInputQty(lotQtyList.get(i).floatValue()); //TODO 얘는진짜주의
            mip.setInputType("produced_in");
            mip.setSourceDataPk(mpEntityList.get(i).getId());
            mip.setSourceTableName("mat_produce");
            mip.setState("confirmed");
            mip.set_status("a");
            mip.setDescription("차수생산입고");
            mip.set_audit(user);
            mip.setSpjangcd(spjangcd);
            this.matInoutRepository.save(mip);

            //mat_lot 의 출고량과 현재고 수량 업데이트
            this.calculate_balance_mat_lot_with_job_res(jr.getId()); //TODO: 성능개선포인트
        }

        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.getJobResponseGoodDefectQty(jrPk);

        float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
        float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());
        jr.setGoodQty(goodQtySum);
        jr.setDefectQty(defectQtySum);
        jr.set_audit(user);

        jr = this.jobResRepository.save(jr);

        Map<String, Object> item = new HashMap<>();
        item.put("jr_pk", jrPk);
        //item.put("lot_number", lotNumber);
        item.put("good_qty_sum", jr.getGoodQty());
        //item.put("chasu", chasu);
        item.put("prod_mat_cd", m.getCode());
        result.data = item;
        return result;
    }

    public List<Map<String, Object>> get_chasu_bom_mat_qty_list(int id) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("id", id);

        String sql = """
	       		with mp as(
		        select 
		        "Material_id"
		        , (COALESCE("GoodQty",0)+COALESCE("DefectQty",0)+COALESCE("ScrapQty",0)+COALESCE("LossQty",0)) as prod_qty
		        , "ProductionDate"
		        from mat_produce
		         where id = :id
		        ), 
		        
		        bom1 as (
		        select 
		        b1.id as bom_pk, 
		        b1."Material_id" as prod_pk
		        , b1."OutputAmount" as produced_qty
		        , mp.prod_qty
		        , row_number() over(partition by b1."Material_id" order by b1."Version" desc) as g_idx
		        from bom b1
		         inner join mp on mp."Material_id"=b1."Material_id"
		        where b1."BOMType" = 'manufacturing' and mp."ProductionDate" between b1."StartDate" and b1."EndDate"  
		        ), 
		        
		        BT as (
		        select 
		        bc."Material_id" as mat_pk
		        , bom1.produced_qty
		        , bc."Amount" as quantity 
		        , bc."Amount" / bom1.produced_qty as bom_ratio
		        , bc."Amount" / bom1.produced_qty * bom1.prod_qty as chasu_bom_qty 
		        from bom_comp bc 
		        inner join bom1 on bom1.bom_pk=bc."BOM_id"
		        where bom1.g_idx = 1
		        )
		        
		        select 
		        BT.mat_pk
		        , mg."MaterialType" as mat_type
		        , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
		        , mg."Name" as mat_group_name
		        , m."Code" as mat_code
		        , m."Name" as mat_name
		        , u."Name" as unit_name
		        , BT.bom_ratio
		        , BT.chasu_bom_qty
		        , coalesce(m."LotUseYN",'N') as "lotUseYn"
		        from BT
		        inner join material m on m.id=BT.mat_pk
		        left join mat_grp mg on mg.id=m."MaterialGroup_id"
		        left join unit u on u.id=m."Unit_id"
				""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    public List<Map<String, Object>> getMaterialProcessInputList(int jrPk, int matPk) {

        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("jrPk", jrPk);
        param.addValue("matPk", matPk);

        String sql = """
                select  mpi.id  as mpi_id
                	  ,	mpi."RequestQty" as req_qty
                	  , mpi."InputQty" as input_qty
                	  , mpi."Material_id" as mat_pk
                	  , ml."CurrentStock" as curr_qty
                	  , ml.id as ml_id
                	  , ml."LotNumber"
                	  , ml."EffectiveDate" as eff_date
                from job_res jr
                inner join mat_proc_input mpi on mpi."MaterialProcessInputRequest_id"  = jr."MaterialProcessInputRequest_id"
                inner join mat_lot ml on ml.id = mpi."MaterialLot_id"
                where jr.id = :jrPk
                and mpi."Material_id" = :matPk
                order by ml."EffectiveDate"
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

        return items;
    }
    //endregion

    /// /chasu_save
    //region : 차수 저장
    @Transactional
    public AjaxResult saveSingleChasu(Integer jrPk, Integer mpId, Float goodQty, Float defectQty, Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        Timestamp now = DateUtil.getNowTimeStamp();
        // 현재 일자
        LocalDate date = LocalDate.now();
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // 현재 시간
        LocalTime time = LocalTime.now();
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss");

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        MaterialProduce mpe = this.matProduceRepository.getMatProduceById(mpId);

        MaterialLot prodMatLot = this.matLotRepository.getByLotNumber(mpe.getLotNumber());

        List<MatLotCons> prodMatLotConsCount = this.matLotConsRepository.findByMaterialLotId(prodMatLot.getId());

        if (!prodMatLotConsCount.isEmpty()) {
            throw new CustomException("해당차수의 로트가 이미 사용되어 수정할 수 없습니다.");
        }

        float mpGoodQty = mpe.getGoodQty() != null ? mpe.getGoodQty() : 0;
        float mpDefectQty = mpe.getDefectQty() != null ? mpe.getDefectQty() : 0;

//		if (Float.compare(mpGoodQty, goodQty) == 0 && Float.compare(mpDefectQty, defectQty) == 0) {	//if (Float.compare(mpe.getGoodQty(), goodQty) == 0 && Float.compare(mpe.getDefectQty(), defectQty) == 0) {
//			result.message = "수량변경이 없습니다.("+	mpe.getLotNumber()+ ")";
//			result.success = false;
//		    return result;
//		}

        MaterialProduce mp = this.matProduceRepository.getMatProduceById(mpId);

        if (mp.getGoodQty() == null) mp.setGoodQty((float) 0);
        if (mp.getDefectQty() == null) mp.setDefectQty((float) 0);

        Float diffGoodQty = goodQty - mp.getGoodQty();
        Float diffDefectQty = defectQty - mp.getDefectQty();
        Float diffTotal = diffGoodQty + diffDefectQty;

        // 1. mat_produce 변경
        Float prevMatProdGoodQty = mp.getGoodQty();
        mp.setGoodQty(goodQty);
        mp.setDefectQty(defectQty);
        mp.setDescription("차수생산 수량변경");
        mp.setActorId(user.getId());
        mp.set_audit(user);
        this.matProduceRepository.saveAndFlush(mp);

        MaterialLot ml = this.matLotRepository.findBySourceTableNameAndSourceDataPkAndLotNumber("mat_produce", mp.getId(), mp.getLotNumber());

        // 2.생산입고 mat_inout 수량 조절
        if (diffGoodQty != 0) {
            MaterialInout mi = this.matInoutRepository.findBySourceTableNameAndSourceDataPkAndInOutAndInputTypeAndMaterialId("mat_produce", mp.getId(), "in", "produced_in", mp.getMaterialId());
            String message = "생산차수수량변경 " + prevMatProdGoodQty + "->" + goodQty;
            mi.setInputQty(mp.getGoodQty());
            mi.setDescription(message);
            mi.setInoutDate(LocalDate.parse(date.format(dateFormat)));
            mi.setInoutTime(LocalTime.parse(time.format(timeFormat)));
            mi = this.matInoutRepository.saveAndFlush(mi);

            ml.setCurrentStock(ml.getCurrentStock() - ml.getInputQty() + mp.getGoodQty());
            ml.setInputQty(mp.getGoodQty());
            ml = this.matLotRepository.saveAndFlush(ml);
        }

        // 합산물량이 변경이 없으면 소모물량은 변경없다
        if (diffTotal == 0) {
            // jobres 양품량 업데이트
            Map<String, Object> mapSum = this.getJobResponseGoodDefectQty(jrPk);

            float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
            float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());

            jr.setGoodQty(goodQtySum);
            jr.setDefectQty(defectQtySum);
            jr.set_audit(user);
            jr = this.jobResRepository.save(jr);

            Map<String, Object> item = new HashMap<String, Object>();
            item.put("jr_pk", jrPk);
            item.put("lot_number", mp.getLotNumber());
            item.put("good_qty_sum", goodQtySum);
            item.put("defect_qty_sum", defectQtySum);

            result.success = true;
            result.data = item;
            return result;
        }

        // 변경된 물량만큼 소모 BOM 조회함
        List<Map<String, Object>> bomMatItems = this.get_chasu_bom_mat_qty_list(mp.getId());

        // mat_lot_cons 삭제 및 mat_lot 정산
        // this.productionResultService.delete_mlc_and_rebalance_ml(mp.getId());

        this.matLotConsRepository.deleteBySourceTableNameAndSourceDataPk("mat_produce", mp.getId());

        for (Map<String, Object> bomMap : bomMatItems) {
            float chasuBomQty = Float.parseFloat(bomMap.get("chasu_bom_qty").toString());
            int consumeMatPk = (int) bomMap.get("mat_pk");
            String matName = bomMap.get("mat_name").toString();
            Material consMat = this.materialRepository.getMaterialById(consumeMatPk);
            String lotUseYn = bomMap.get("lotUseYn").toString();

            // 3.변경된 물량 만큼 consume 물량 변경
            MaterialConsume mc = this.matConsuRepository.getByJobResponseIdAndProcessOrderAndLotIndexAndMaterialId(jr.getId(), mp.getProcessOrder(), mp.getLotIndex(), consumeMatPk);
            mc.setBomQty(chasuBomQty);
            mc.setConsumedQty(chasuBomQty);
            mc.set_audit(user);
            mc = this.matConsuRepository.saveAndFlush(mc);

            // mat_inout 물량 조정
            MaterialInout mi = this.matInoutRepository.findBySourceTableNameAndSourceDataPkAndInOutAndOutputTypeAndMaterialId("mat_consu", mc.getId(), "out", "consumed_out", consumeMatPk);
            mi.set_audit(user);
            mi.setDescription("'차수생산수량변경" + mi.getOutputQty() + " -> " + chasuBomQty);
            mi.setOutputQty(chasuBomQty);
            mi = this.matInoutRepository.saveAndFlush(mi);

            if ("Y".equals(lotUseYn)) {
                // 수정시작
                // 1. mat_proc_input 에서 해당 품목의 로트리스트를 가져온다.

                List<Map<String, Object>> mpiList = this.getMaterialProcessInputList(jr.getId(), consumeMatPk);
                // 투입요청에서 해당 품목이 로트 투입인지 조회한다

                float totalLotQty = 0;
                for (int j = 0; j < mpiList.size(); j++) {
                    Map<String, Object> mpiMap = mpiList.get(j);

                    float currQty = Float.parseFloat(mpiMap.get("curr_qty").toString());
                    totalLotQty += currQty;
                }

                if (totalLotQty < chasuBomQty) {
                    throw new CustomException("가용한 LOT 재고가 없습니다.(" + matName + ")\n 투입 내역에서 가용 재고를 추가해주세요. ");
                }

                // 작업준비에 설정된 lot 투입 품목이면
                // 로트 사용량 추가
                float remainQty = chasuBomQty;

                // MaterialProcessInput 조회
                for (int k = 0; k < mpiList.size(); k++) {
                    Map<String, Object> mpiMap = mpiList.get(k);
                    int matLotId = (int) mpiMap.get("ml_id");
                    float currentStock = Float.parseFloat(mpiMap.get("curr_qty").toString());
                    if (currentStock == 0) {
                        continue;
                    }

                    MatLotCons mlc = new MatLotCons();
                    mlc.setMaterialLotId(matLotId);
                    mlc.setOutputDateTime(now);
                    mlc.setSourceDataPk(mp.getId());
                    mlc.setSourceTableName("mat_produce");
                    mlc.set_audit(user);
                    mlc.setCurrentStock(ml.getCurrentStock()); // 당시 재고량

                    if (currentStock >= remainQty) {
                        // 해당로트의현재수량 가능
                        mlc.setOutputQty(remainQty);
                        remainQty = (float) 0;
                        mlc = this.matLotConsRepository.save(mlc);

                        break;
                    } else {
                        mlc.setOutputQty(currentStock);
                        mlc = this.matLotConsRepository.save(mlc);
                        remainQty = remainQty - currentStock;
                    }

                }

//                if (remainQty > 0) {
//                    result.message = "로트 수량이 부족합니다.(" + matName + ")";
//                    result.success = false;
//                    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
//                    return result;
//                }
            } else {
                if ("1".equals(consMat.getUseyn())) {
                    throw new CustomException("\"사용 불가능한 품목이 BOM에 등록되어 있습니다.(\" + matName + \")\"");
                }

                // mtyn이 0일 때는 재고 체크하지 않음
                if ("0".equals(consMat.getMtyn())) {
                    // 아무 조건 없이 통과
                } else {
                    Float currentStock = consMat.getCurrentStock();
                    if (currentStock == null || currentStock == 0f) {
                        result.message = "가용한 품목 재고가 없습니다.(" + matName + ")";
                        result.success = false;
                        TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                        return result;
                    } else if (currentStock < goodQty) {
                        result.message = "가용한 품목 재고가 부족합니다. \n(" +
                                matName + ", 필요 수량: " + goodQty + ", 가용 수량: " + currentStock + ")";
                        result.success = false;
                        TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                        return result;
                    }
                }
            }
        }
        // 한번더 정산
        //this.productionResultService.calculate_balance_mat_lot_with_mat_prod(mp.getId());
        this.calculate_balance_mat_lot_with_job_res(jr.getId());
        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.getJobResponseGoodDefectQty(jrPk);

        float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
        float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());

        jr.setGoodQty(goodQtySum);
        jr.setDefectQty(defectQtySum);
        jr.set_audit(user);
        jr = this.jobResRepository.save(jr);

        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_pk", jrPk);
        item.put("lot_number", mp.getLotNumber());
        item.put("good_qty_sum", goodQtySum);
        item.put("defect_qty_sum", defectQtySum);

        result.data = item;
        result.success = true;


        return result;
    }


    /// //////////////////////////////////////////////////////////////////////////////////work_finish
    // region : 작업 완료
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> finishWork(WorkFinishRequest req, User user) {
        JobRes jr = jobResRepository.getJobResById(req.getId());
        validator.throwIfNull(jr, "작업지시가 존재하지 않습니다.");

        /* =========================
         * 1. 검증 파라미터 구성
         * ========================= */
        Map<String, Object> validationParam = buildValidationParam(req);

        /* =========================
         * 2. 차수 자동 생성 (부수공정) / 부모 -> 사용자가 직접  ,  부수공정 -> 자동으로 완료하면 생산저장
         * ========================= */
        boolean isMainProcess = jr.getParentId() == null;
        if (!isMainProcess) {
            this.chasu_add_service(
                    jr.getId(),
                    jr.getOrderQty(),
                    jr.getSpjangcd(),
                    user
            );
        }

        /* =========================
         * 3. 생산/투입 조회
         * ========================= */
        List<MaterialConsume> mcList =
                matConsuRepository.findByJobResponseId(jr.getId());

        List<MaterialProduce> mpList =
                matProduceRepository.findByJobResponseId(jr.getId());

        /* =========================
         * 4. 작업완료 검증
         * ========================= */
        Timestamp endTime =
                this.workFinish_validation(
                        jr, validationParam, mcList, mpList
                );

        /* =========================
         * 5. JobRes 업데이트
         * ========================= */
        applyFinishToJobRes(jr, req, endTime, user);

        //불량창고에 불량품 등록
        this.add_jobres_defectqty_inout(
                jr.getId(), user.getId()
        );

        jobResRepository.save(jr);


        /* =========================
         * 6. 다음 공정 생성
         * ========================= */
        createNextProcess(jr, user);

        /* =========================
         * 7. 설비 종료
         * ========================= */
        finishEquipmentRun(jr, endTime, user);

        /* =========================
         * 8. 결과 반환
         * ========================= */
        Map<String, Object> result = new HashMap<>();
        result.put("jr_pk", jr.getId());
        return result;

    }

    private Map<String, Object> buildValidationParam(WorkFinishRequest req) {
        Map<String, Object> map = new HashMap<>();
        map.put("endDate", req.getEnd_date());
        map.put("endTime", req.getEnd_time());
        map.put("prodDate", req.getProd_date());
        map.put("startTime", req.getStart_time());
        return map;
    }


    private void applyFinishToJobRes(
            JobRes jr,
            WorkFinishRequest req,
            Timestamp endTime,
            User user) {

        jr.set_audit(user);
        jr.setLotNumber(req.getLot_num());
        //jr.setGoodQty(req.getGood_qty());
        jr.setGoodQty(req.getOrder_qty());
        jr.setDefectQty(req.getDefect_qty());
        jr.setLossQty(req.getLoss_qty());
        jr.setScrapQty(req.getScrap_qty());
        jr.setProductionDate(CommonUtil.tryTimestamp(req.getProd_date()));
        jr.setEndDate(Date.valueOf(req.getEnd_date()));
        jr.setEndTime(endTime);
        jr.setShiftCode(req.getShift_code());
        jr.setWorkCenter_id(req.getWorkcenter_id());
        jr.setEquipment_id(req.getEquipment_id());
        jr.setDescription(req.getDescription());
        jr.setState("finished");
    }


    private void finishEquipmentRun(
            JobRes jr,
            Timestamp endTime,
            User user) {

        equRunRepository
                .findLatestRunningByEquipmentAndOrder(
                        jr.getEquipment_id(), jr.getWorkOrderNumber()
                )
                .ifPresent(equ -> {
                    equ.setEndDate(endTime);
                    equ.setRunState("complete");
                    equ.setSourceTableName("job_res");
                    equ.setSourceDataPk(jr.getId());
                    equ.set_audit(user);
                    equRunRepository.save(equ);
                });
    }

    private void createNextProcess(JobRes jr, User user) {

        JobResProcessTree jpt = jobResProcessTreeRepository.findByWorkOrderNo(jr.getWorkOrderNumber());
        if(jpt == null) throw new CustomException("해당 작업지시에 대한 공정을 찾을 수 없습니다.");

        String processTree = jpt.getProcessTree();

        Integer matId = jr.getMaterialId();

        //다음 공정 노드 찾기
        BomNode next =
                this.completeAndGetParentNode(
                        JsonUtil.parseProcessTree(processTree), matId
                );

        //담 공정 없으면 스킵
        if(next == null) return;

        // 다음 공정트리 구하기
        String nextTree =
                new BomTreeService()
                        .returnProcessTreeCurrentNode(processTree, next.matPk);

        if(next.matPk != null){
            this.saveNextOfProcess(
                    jr, next, user
            );
        }

        // job_res_tree도 최신화
        // process Tree 도 최신화
        int cnt = jobResProcessTreeRepository.updateProcessTreeOnly(jr.getWorkOrderNumber(), nextTree);
        if(cnt <= 0) throw new CustomException("해당 작업지시에 대한 공정을 찾을 수 없습니다.");



    }
    //////////////////////////////////////////////////////////////////////////////////
    //endregion
}
