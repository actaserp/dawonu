package mes.app.production;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.IntStream;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.JsonNode;
import mes.Exception.CustomException;
import mes.app.definition.service.BomService;
import mes.app.definition.service.EquipmentService;
import mes.app.production.ProductuibResult_validation.ProductionResultValidator;

import mes.app.production.dto.productionResult.WorkFinishRequest;
import mes.app.production.production_package.BomNode;
import mes.app.production.production_package.BomTreeService;
import mes.app.production.production_package.ProcessFlow;
import mes.app.production.service.EquipmentRunChartService;
import mes.app.util.JsonUtil;
import mes.app.util.UtilClass;
import mes.domain.entity.*;
import mes.domain.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import mes.app.inventory.service.LotService;
import mes.app.production.service.ProductionResultService;
import mes.domain.model.AjaxResult;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;


@RestController
@RequestMapping("/api/production/prod_result")
public class ProductionResultController {

    @Autowired
    private ProductionResultService productionResultService;

    @Autowired
    private JobResProcessTreeRepository jobResProcessTreeRepository;

    @Autowired
    ProductionResultValidator validator;

    @Autowired
    MatConsuRepository matConsuRepository;

    @Autowired
    JobResRepository jobResRepository;

    @Autowired
    MatProcInputReqRepository matProcInputReqRepository;

    @Autowired
    JobResDefectRepository jobResDefectRepository;

    @Autowired
    MatProduceRepository matProduceRepository;

    @Autowired
    MaterialRepository materialRepository;

    @Autowired
    StorehouseRepository storehouseRepository;

    @Autowired
    SystemOptionRepository systemOptionRepository;

    @Autowired
    MatLotRepository matLotRepository;

    @Autowired
    MatProcInputRepository matProcInputRepository;

    @Autowired
    MatLotConsRepository matLotConsRepository;

    @Autowired
    MatInoutRepository matInoutRepository;

    @Autowired
    TestResultRepository testResultRepository;

    @Autowired
    TestItemResultRepository testItemResultRepository;

    @Autowired
    EquipmentService equipmentService;

    @Autowired
    EquRunRepository equRunRepository;


    @GetMapping("/read")
    public AjaxResult getProdResult(
            @RequestParam(value = "date_from", required = false) String dateFrom,
            @RequestParam(value = "date_to", required = false) String dateTo,
            @RequestParam(value = "is_include_comp", required = false) String isIncludeComp,
            @RequestParam(value="factory", required=false) Integer cboFactory,
            @RequestParam(value = "choMat", required = false) String choMat,
            @RequestParam(required = false) Integer job_proc,
            @RequestParam("spjangcd") String spjangcd) {

        List<Map<String, Object>> items = this.productionResultService.getProdResult(dateFrom, dateTo, isIncludeComp, spjangcd, choMat, cboFactory, job_proc);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    @GetMapping("/detail")
    public AjaxResult getProdResultDetail(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk) {

        Map<String, Object> items = this.productionResultService.getProdResultDetail(jrPk);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    @GetMapping("/mat_detail")
    public AjaxResult getProdResultMatDetail(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk) {

        Map<String, Object> items = this.productionResultService.getProdResultMatDetail(jrPk);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    @GetMapping("/print_detail")
    public AjaxResult getProdResultPrintDetail(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk) {

        Map<String, Object> items = this.productionResultService.getProdResultPrintDetail(jrPk);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    //TODO: 얘 근데 뭔가 헷갈림, 제품-반제품-원자재 구성일때 원자재 BOM은 안보여줌.
    @GetMapping("/process-step-meta")
    public AjaxResult getProcessStepMeta(
            @RequestParam Integer material_id,
            @RequestParam Integer routing_id,
            @RequestParam Integer process_id,
            @RequestParam(required=false) BigDecimal order_qty,
            @RequestParam(required=false) String prod_date
    ){
        Map<String,Object> data = productionResultService.getProcessStepMeta(routing_id, process_id, material_id, order_qty, prod_date);
        AjaxResult r = new AjaxResult();
        r.data = data;


        return r;
    }

    //todo: 어디서 쓰는겨? 위치를 못찾겠네
    @GetMapping("/consumed_list_by_process")
    public AjaxResult getConsumedByProcess(
            @RequestParam Integer material_id,
            @RequestParam Integer routing_id,
            @RequestParam Integer process_id,
            @RequestParam BigDecimal order_qty,
            @RequestParam String prod_date
    ){
        List<Map<String,Object>> rows = productionResultService.getConsumedByProcess(routing_id, process_id, material_id, order_qty, prod_date);
        AjaxResult r = new AjaxResult();
        r.data = rows;
        return r;
    }


    //부적합 목록 조회
    @GetMapping("/defect_list")
    public AjaxResult getDefectList(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk, @RequestParam(value = "workcenter_id", required = false) Integer workcenterId) {

        List<Map<String, Object>> items = this.productionResultService.getDefectList(jrPk, workcenterId);
        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    //차수별 생산. mat_produce(생산실적입력,LOT) 테이블이 관여함.
    @GetMapping("/chasu_list")
    public AjaxResult getChasuList(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk) {

        List<Map<String, Object>> items = this.productionResultService.getChasuList(jrPk);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    // 공정에 투입된 품목들의 LOT별 상세 현황과 실제 소비된 수량을 조회
    @GetMapping("/input_lot_list")
    public AjaxResult getInputLotList(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "mat_code", required = false) String mat_code) {

        List<Map<String, Object>> items = this.productionResultService.getInputLotList(jrPk, mat_code);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    //뭔진모르겠지만 간단한거임
    @GetMapping("/find-by-order-process")
    public AjaxResult findJobByOrderAndProcess(
            @RequestParam String order_num,
            @RequestParam Integer process_id,
            @RequestParam Integer pro_mat_id
    ){
        Integer jrPk = productionResultService.findJobByOrderAndProcess(order_num, process_id, pro_mat_id);
        AjaxResult r = new AjaxResult();
        r.data = (jrPk != null) ? Map.of("jr_pk", jrPk) : null;
        return r;
    }

    //투입목록 조회
    @GetMapping("/consumed_list")
    public AjaxResult getConsumedList(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "mat_pk", required = false) Integer materialId,
            @RequestParam(value = "process_id", required = false) Integer processId,
            @RequestParam(value = "routing_id", required = false) Integer routingId,
            @RequestParam(value = "order_qty", required = false) BigDecimal order_qty,
            @RequestParam(value = "prod_date", required = false) String prodDate,
            @RequestParam(value = "prod_mat_id", required = false) Integer prod_mat_id,
            @RequestParam(value = "need_pro_mat_qty", required = false) BigDecimal need_pro_mat_qty,
            @RequestParam(value = "consumed_mode", required = false) String consumed_mode) {


        List<Map<String, Object>> items;

        if ("PLAN".equalsIgnoreCase(consumed_mode)) {
            // 공정 시작 전(예상): pro_mat_id + need_pro_mat_qty로 소요 계산
            items = this.productionResultService.getConsumedListPlan(prod_mat_id, need_pro_mat_qty, prodDate);

        } else{
            items = this.productionResultService.getConsumedListFirst(jrPk, materialId, prodDate);
        }

        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    //생산실적 기본정보 저장
    @PostMapping("/save")
    @Transactional
    public AjaxResult saveProdResult(
            @RequestParam(value = "id", required = false) Integer jrPk,
            @RequestParam(value = "lot_num", required = false) String lotNum,
            @RequestParam(value = "good_qty", required = false) Float goodQty,
            @RequestParam(value = "defect_qty", required = false) Float defectQty,
            @RequestParam(value = "loss_qty", required = false) Float lossQty,
            @RequestParam(value = "scrap_qty", required = false) Float scrapQty,
            @RequestParam(value = "shift_code", required = false) String shiftCode,
            @RequestParam(value = "workcenter_id", required = false) Integer workcenterId,
            @RequestParam(value = "equipment_id", required = false) Integer equipmentId,
            @RequestParam(value = "prod_date", required = false) String prodDate,
            @RequestParam(value = "end_date", required = false) String endDate,
            @RequestParam(value = "start_time", required = false) String startTime,
            @RequestParam(value = "end_time", required = false) String endTime,
            @RequestParam(value = "description", required = false) String description,
            @RequestParam(value = "mat_pk", required = false) Integer matPk,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();

        Timestamp start_time = null;
        Timestamp end_time = null;
        Timestamp prod_date = CommonUtil.tryTimestamp(prodDate);

        if (!startTime.equals("")) {
            start_time = Timestamp.valueOf(prodDate + ' ' + startTime + ":00");
        } else {
            start_time = null;
        }

        if (!endTime.equals("")) {
            end_time = Timestamp.valueOf(prodDate + ' ' + endTime + ":00");
        } else {
            end_time = null;
        }

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        jr.setLotNumber(lotNum);
        jr.setGoodQty(CommonUtil.tryFloatNull(goodQty));
        jr.setDefectQty(CommonUtil.tryFloatNull(defectQty));
        jr.setLossQty(CommonUtil.tryFloatNull(lossQty));
        jr.setScrapQty(CommonUtil.tryFloatNull(scrapQty));
        jr.setProductionDate(prod_date);
        jr.setStartTime(start_time);
        // 임시로 추가 ------
        if (jr.getOrderQty() == null) jr.setOrderQty((float) 0);
        if (jr.getFirstWorkCenter_id() == null) jr.setFirstWorkCenter_id(workcenterId);
        if (jr.getProductionPlanDate() == null) jr.setProductionPlanDate(prod_date);
        if (jr.getMaterialId() == null) jr.setMaterialId(matPk);
        // -------------
        jr.setEndTime(end_time);
        jr.setEndDate(Date.valueOf(endDate));
        jr.setShiftCode(shiftCode);
        jr.setWorkCenter_id(workcenterId);
        jr.setEquipment_id(equipmentId);
        jr.setDescription(description);
        jr.set_audit(user);
        jr = this.jobResRepository.save(jr);


        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_pk", jrPk);

        result.success = true;
        result.data = item;

        return result;
    }

    // TODO: 작업 시작시에 이전 공정이 다 끝났는지 체크해보자.
    @PostMapping("/work_start")
    @Transactional
    public AjaxResult workStart(
            @RequestParam(value = "id", required = false) Integer jrPk,
            @RequestParam(value = "prod_date", required = false) String prodDate,
            @RequestParam(value = "end_date", required = false) String endDate,
            @RequestParam(value = "start_time", required = false) String startTime,
            @RequestParam(value = "end_time", required = false) String endTime,
            @RequestParam(value = "good_qty", required = false) String goodQty,
            @RequestParam(value = "defect_qty", required = false) String defectQty,
            @RequestParam(value = "loss_qty", required = false) String lossQty,
            @RequestParam(value = "scrap_qty", required = false) String scrapQty,
            @RequestParam(value = "shift_code", required = false) String shiftCode,
            @RequestParam(value = "workcenter_id", required = false) Integer workcenterId,
            @RequestParam(value = "equipment_id", required = false) Integer equipmentId,
            @RequestParam(value = "description", required = false) String description,
            @RequestParam(value = "mat_pk", required = false) Integer matPk,
            @RequestParam(value = "order_num", required = false) String orderNum,
            @RequestParam(value = "prod_mat_id", required = false) Integer prodMatId,
            @RequestParam(value = "process_id", required = false) Integer processId,
            @RequestParam(value = "need_pro_mat_qty", required = false) BigDecimal needProMatQty,
            @RequestParam(value = "consumed_mode", required = false) String consumedMode,
            @RequestParam("spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();

        // region validation check 시작********************************************************************/
        // 공통 시간 세팅
        if (prodDate == null || prodDate.isBlank()) {
            throw new CustomException("생산일이 없습니다.");
        }
        Timestamp start_ts = Timestamp.valueOf(prodDate + " " + startTime + ":00");
        Timestamp end_ts   = (endTime != null && !endTime.isEmpty())
                ? Timestamp.valueOf(prodDate + " " + endTime + ":00")
                : null;
        Timestamp prod_ts  = CommonUtil.tryTimestamp(prodDate);

        //이전 차수의 작업이 완료되었는가?
        List<JobRes> previousJob = this.jobResRepository.getJobResByParentId(jrPk);

        if(!previousJob.isEmpty()){
            for(JobRes job : previousJob){
                String state = job.getState();
                if(!state.equals("finished")){
                    throw new CustomException("아직 이전 공정이 끝나지 않았습니다.");
                }
            }
        }

        // 설비 중복 가동 체크
        long runningCount = this.equRunRepository.countByEquipmentIdAndRunState(equipmentId, "run");
        if (runningCount > 0) throw new CustomException("해당 설비는 이미 가동중입니다.");

        // endregion check 끝********************************************************************/


        JobRes target; // 실제로 start 상태로 저장할 대상(자식 또는 기존)
        if ("PLAN".equalsIgnoreCase(consumedMode)) {
            // PLAN: 새 자식 job_res 생성
            if (jrPk == null || prodMatId == null || needProMatQty == null) {
                throw new CustomException("\"PLAN 모드에는 부모작지/공정산출품/지시수량이 필요합니다.\"");
            }

            JobRes parent = this.jobResRepository.getJobResById(jrPk);
            if (parent == null) {
                result.success = false;
                result.message = "부모 작업지가 없습니다.";
                return result;
            }

            Material m = materialRepository.getMaterialById(prodMatId);
            Integer locPk = m.getStoreHouseId();

            // (중복 방지) 동일 WO + 동일 공정 + 동일 산출품 자식이 이미 있으면 재사용
            Integer dupId = this.jobResRepository.findIdByOrderProcessAndMaterial(orderNum, processId, prodMatId);
            if (dupId != null) {
                target = this.jobResRepository.getJobResById(dupId);
            } else {
                // 필요 시 공정 순서 조회

                target = new JobRes();
                target.setWorkOrderNumber(orderNum != null ? orderNum : parent.getWorkOrderNumber());
                target.setParentId(parent.getId());
                target.setMaterialId(prodMatId);
                target.setOrderQty(needProMatQty.floatValue());
                target.setWorkCenter_id(workcenterId);
                target.setEquipment_id(equipmentId);
                target.setProductionDate(prod_ts);
                target.setProductionPlanDate(prod_ts);
                target.setFirstWorkCenter_id(
                        parent.getFirstWorkCenter_id() != null ? parent.getFirstWorkCenter_id() : workcenterId);
                target.setDescription(description);
                target.setShiftCode(shiftCode);
                target.setState("working");                          // 바로 시작
                target.setStartTime(start_ts);
                target.setSpjangcd(spjangcd);
                if (endDate != null && !endDate.isEmpty()) target.setEndDate(Date.valueOf(endDate));
                target.set_audit(user);
                target.setStoreHouse_id(locPk);
                target.setRouting_id(parent.getRouting_id());
                target.setWorkIndex(parent.getWorkIndex());

                // 투입요청 생성(최초 1회)
                MatProcInputReq mir = new MatProcInputReq();
                mir.setRequestDate(DateUtil.getNowTimeStamp());
                mir.setRequesterId(user.getId());
                mir.set_audit(user);

                if(1==1) throw new RuntimeException();

                mir = this.matProcInputReqRepository.save(mir);
                target.setMaterialProcessInputRequestId(mir.getId());

                target = this.jobResRepository.save(target);
            }

        } else {
            // ✅ ACTUAL: 기존 job_res 업데이트
            if (jrPk == null) {
                result.success = false;
                result.message = "작업지 id가 없습니다.";
                return result;
            }
            target = this.jobResRepository.getJobResById(jrPk);
            if (target == null) {
                result.success = false;
                result.message = "작업지를 찾을 수 없습니다.";
                return result;
            }

            // 최초 투입요청 연결
            if (target.getMaterialProcessInputRequestId() == null) {
                MatProcInputReq mir = new MatProcInputReq();
                mir.setRequestDate(DateUtil.getNowTimeStamp());
                mir.setRequesterId(user.getId());
                mir.set_audit(user);
                mir = this.matProcInputReqRepository.save(mir);
                target.setMaterialProcessInputRequestId(mir.getId());
            }

            // 상태/시간/기본값 보정
            if (target.getOrderQty() == null) target.setOrderQty(0f);
            if (target.getFirstWorkCenter_id() == null) target.setFirstWorkCenter_id(workcenterId);
            if (target.getProductionPlanDate() == null) target.setProductionPlanDate(prod_ts);
            if (target.getMaterialId() == null) target.setMaterialId(matPk);

            target.setState("working");
            target.setProductionDate(prod_ts);
            target.setStartTime(start_ts);
            target.setEndTime(end_ts);
            if (endDate != null && !endDate.isEmpty()) target.setEndDate(Date.valueOf(endDate));
            target.setShiftCode(shiftCode);
            target.setWorkCenter_id(workcenterId);
            target.setEquipment_id(equipmentId);
            target.setDescription(description);
            target.set_audit(user);

            target = this.jobResRepository.save(target);
        }

        // 설비 가동 시작 로그
        EquRun er = new EquRun();
        er.setEquipmentId(equipmentId);
        er.setStartDate(start_ts);
        er.setWorkOrderNumber(orderNum != null ? orderNum : target.getWorkOrderNumber());
        er.setRunState("run");
        er.setSourceTableName("job_res");
        er.setSourceDataPk(jrPk);
        er.set_audit(user);
        er.setSpjangcd(spjangcd);
        this.equRunRepository.save(er);

        // 응답: 프론트에서 res.data.jr_pk를 쓰니 id만 내려주자
        AjaxResult r = new AjaxResult();
        r.success = true;
        r.data = java.util.Map.of("jr_pk", target.getId());
        return r;
    }

    //부적합내역 저장
    @PostMapping("/defect_save")
    @Transactional
    public AjaxResult defectSave(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam("spjangcd") String spjangcd,
            @RequestBody MultiValueMap<String, Object> defect_list,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();

        List<Map<String, Object>> items = CommonUtil.loadJsonListMap(defect_list.getFirst("defect_list").toString());

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        JobResDefect jrd = null;

        //일단 다 삭제
        jobResDefectRepository.deleteByJobResponseId(jrPk);

        for (int i = 0; i < items.size(); i++) {

            Integer defectId = Integer.parseInt(items.get(i).get("defect_id").toString());
            Float defectQty = Float.parseFloat(items.get(i).get("defect_qty").toString());
            String defectRemark = items.get(i).get("defect_remark") != null ? items.get(i).get("defect_remark").toString() : null;

            jrd = this.jobResDefectRepository.findByJobResponseIdAndDefectTypeId(jrPk, defectId);

            if (jrd == null) { //없으면 추가
                jrd = new JobResDefect();
                jrd.setJobResponseId(jrPk);
                jrd.setDefectTypeId(defectId);
                jrd.setDefectQty(defectQty);
                jrd.setDescription(defectRemark);
                jrd.setProcessOrder(0);
                jrd.setLotIndex(0);
                jrd.set_audit(user);
                jrd.setSpjangcd(spjangcd);
                this.jobResDefectRepository.save(jrd);

            } else {
                jrd.setDefectQty(defectQty);
                jrd.setDescription(defectRemark);
                jrd.set_audit(user);
                jrd.setSpjangcd(spjangcd);
                this.jobResDefectRepository.save(jrd);

            }
        }


        List<JobResDefect> jrdList = this.jobResDefectRepository.findByJobResponseId(jrPk);

        Float jobresTotalDefectQty = (float) 0;

        for (JobResDefect sum : jrdList) {
            jobresTotalDefectQty += sum.getDefectQty();
        }

        jr.setDefectQty(jobresTotalDefectQty);
        jr.set_audit(user);

        jr = this.jobResRepository.save(jr);

        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_pk", jrPk);
        item.put("total_defect", jobresTotalDefectQty);

        float chasu_defect_qty = this.productionResultService.getChasuDefectQty(jrPk);

        // 차수에 등록된 부적합품이랑 부적합 텝의 총합계 비교
        if (Float.compare(chasu_defect_qty, Float.parseFloat(jobresTotalDefectQty.toString())) != 0) {
            result.message = "차수별 생산의 부적합량 합계와 값이 일치하지 않습니다";
            result.success = false;
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return result;
        }

        result.success = true;
        result.data = item;
        return result;
    }

    @PostMapping("/work_finish")
    public AjaxResult workFinish(@ModelAttribute WorkFinishRequest req,
            Authentication auth) {

        User user = (User) auth.getPrincipal();

        Map<String, Object> result = productionResultService.finishWork(req, user);

        return AjaxResult.success(null, result);
    }


    /**
     * 1. 작업지시의 entTime = null , State는 working
     * 2. mat_inout에서 불량품으로 등록된 것들 삭제
     * 3. 설비동작 이력을 가져와서 RunState를 "complete_cancel" 로 수정후 새로운 행을 하나 추가(RunState : run)
     * */
    @PostMapping("/finish_cancel")
    public AjaxResult finishCancel(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "spjangcd", required = false) String spjangcd,
            @RequestParam(value = "Equipment_id", required = false) Integer Equipment_id,
            HttpServletRequest request,
            Authentication auth) {

        User user = (User) auth.getPrincipal();

        //통합 메서드
        Map<String, Object> item = productionResultService.finishCancel(jrPk, user, spjangcd);

        return AjaxResult.success(null, item);
    }




    //사용하는 곳을 못찾겠음.
    @PostMapping("/consumed_save")
    @Transactional
    public AjaxResult consumedSave(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "mp_pk", required = false) String mpPk,
            @RequestParam(value = "prod_date", required = false) String prodDate,
            @RequestParam(value = "bom_output_amount", required = false) String bomOutputAmount,
            @RequestBody MultiValueMap<String, Object> Q,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();

        List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

        if (!mpPk.equals("")) {
            MaterialProduce mp = this.matProduceRepository.getMatProduceById(Integer.parseInt(mpPk));

            if (mp != null) {
                mp.setBomOutputAmount(bomOutputAmount.equals("") || bomOutputAmount == null ? null : Float.parseFloat(bomOutputAmount));
                mp = this.matProduceRepository.save(mp);
            }
        }

        SystemOption so = this.systemOptionRepository.getByCode("consume_from_house_option");

        String consumeHouseOption = "master";

        Integer baseStorehouseId = null;

        if (so.getCode().equals("process")) {
            consumeHouseOption = "process";
            List<StoreHouse> shList = this.storehouseRepository.findByHouseType("process");
            if (shList.size() > 0) {
                baseStorehouseId = Integer.parseInt(shList.get(0).getId().toString());
            }
        }

        for (int i = 0; i < items.size(); i++) {
            Integer matPk = Integer.parseInt(items.get(i).get("mat_pk").toString());
            Float bomConsumed = items.get(i).get("bom_consumed").equals("") ? 0 : Float.parseFloat(items.get(i).get("bom_consumed").toString());
            Float consumed = items.get(i).get("consumed_qty").equals("") ? 0 : Float.parseFloat(items.get(i).get("consumed_qty").toString());
            String consumedStart = items.get(i).get("consumed_start").equals("") ? null : prodDate + ' ' + items.get(i).get("consumed_start").toString() + ":00";
            String consumedEnd = items.get(i).get("consumed_end").equals("") ? null : prodDate + ' ' + items.get(i).get("consumed_end").toString() + ":00";

            Float totalConsumed = consumed;

            Float addConsumed = totalConsumed - bomConsumed;

            Integer storehouseId = null;
            if (baseStorehouseId != null) {
                storehouseId = baseStorehouseId;
            } else if (consumeHouseOption.equals("master")) {
                Material m = this.materialRepository.getMaterialById(matPk);
                storehouseId = (int) Math.floor(m.getStoreHouseId());
            }

            List<MaterialConsume> mcList = this.matConsuRepository.findByJobResponseIdAndMaterialId(jrPk, matPk);

            if (mcList.size() == 0) {
                MaterialConsume mc = new MaterialConsume();
                mc.setJobResponseId(jrPk);
                mc.setMaterialId(matPk);
                mc.setProcessOrder(0);
                mc.setLotIndex(0);
                mc.setBomQty(bomConsumed);
                mc.setConsumedQty(totalConsumed);
                mc.setAddQty(addConsumed);
                mc.setStartTime(consumedStart == null ? null : Timestamp.valueOf(consumedStart));
                mc.setEndTime(consumedEnd == null ? null : Timestamp.valueOf(consumedEnd));
                mc.setStoreHouseId(storehouseId);
                mc.set_audit(user);
                mc = this.matConsuRepository.save(mc);

            } else {
                for (int j = 0; j < mcList.size(); j++) {
                    MaterialConsume mc = mcList.get(j);
                    mc.setBomQty(bomConsumed);
                    mc.setConsumedQty(totalConsumed);
                    mc.setAddQty(addConsumed);
                    mc.setStartTime(consumedStart == null ? null : Timestamp.valueOf(consumedStart));
                    mc.setEndTime(consumedEnd == null ? null : Timestamp.valueOf(consumedEnd));
                    mc.setStoreHouseId(storehouseId);
                    mc.set_audit(user);
                    mc = this.matConsuRepository.save(mc);
                }
            }
        }

        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_Pk", jrPk);

        result.success = true;
        result.data = item;

        return result;
    }

    //투입내역 -> 로트 투입 저장 버튼
    /**
     * 1. 해당 작업지시와 품목lot 조회
     * 2. 할당된 품목lot 있는지, 가용한 재고인지, 기본창고가 지정되어 있는지, 이미 지정된 로트인지 validation check
     * 3. MatProcInputReq(투입공정 투입 요청 엔티티 (요청 흔적 같은 느낌? 별 기능은 없는듯))  생성
     * 4. mat_proc_input(제품 생산 투입내역) 에 MatProcInputReq을 넣고, 상태값 "requested" 로 객체 생성
     * **/
    @PostMapping("/add_lot_input")
    @Transactional
    public AjaxResult addLotInput(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "mp_pk", required = false) String mpPk,
            @RequestParam(value = "lot_id", required = false) Integer lotId,
            @RequestParam(value = "input_qty", required = false) Float inputQty,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();

        Timestamp inoutTime = DateUtil.getNowTimeStamp();

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        MaterialLot ml = this.matLotRepository.getMatLotById(lotId);

        if(ml == null) throw new CustomException("할당된 품목 LOT가 존재하지 않습니다.");
        validator.addLotInput_validateByMatLot(ml, "가용한 재고가 없는 LOT을 지정했습니다.(" + ml.getLotNumber() + ")",
                                                  "해당 품목의 기본창고가 지정되지 않았습니다(" + ml.getLotNumber() + ")");


        List<MatProcInput> mpiList = this.matProcInputRepository.findByMaterialProcessInputRequestIdAndMaterialLotId(jr.getMaterialProcessInputRequestId(), ml.getId());
        Integer mpiCount = mpiList.size();
        validator.throwIfExists(mpiCount, "이미 지정된 로트입니다.(" + ml.getLotNumber() + ")");

        MatProcInputReq mir = null; //투입공정 투입 요청 엔티티 (요청 흔적 같은 느낌? 별 기능은 없는듯)

        if (jr != null) {
            if (jr.getMaterialProcessInputRequestId() == null) {
                mir = new MatProcInputReq();
                mir.setRequestDate(inoutTime);
                mir.setRequesterId(user.getId());
                mir.set_audit(user);
                mir = this.matProcInputReqRepository.save(mir);
                jr.setMaterialProcessInputRequestId(mir.getId());
            } else {
                mir = this.matProcInputReqRepository.getMatProcInputReqById(jr.getMaterialProcessInputRequestId());
            }
        }

        MatProcInput mpi = new MatProcInput();
        mpi.setMaterialProcessInputRequestId(mir.getId());
        mpi.setMaterialId(ml.getMaterialId());
        mpi.setRequestQty(inputQty);
        mpi.setInputQty((float) 0);
        mpi.setMaterialLotId(ml.getId());
        mpi.setMaterialStoreHouseId(ml.getStoreHouseId());
        mpi.setState("requested");
        mpi.setInputDateTime(inoutTime);
        mpi.setActorId(user.getId());
        mpi.set_audit(user);
        mpi = this.matProcInputRepository.save(mpi);

        result.success = true;
        result.data = mpi;

        return result;
    }

    //addLotInput랑 비슷한데 얘는 여러항목 투입을 처리
    @PostMapping("/multi_add_lot_input")
    @Transactional
    public AjaxResult multiAddLotInput(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "mp_pk", required = false) String mpPk,
            @RequestBody MultiValueMap<String, Object> Q,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();

        Timestamp inoutTime = DateUtil.getNowTimeStamp();

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> lotMap = data.get(i);

            int lotId = Integer.parseInt(lotMap.get("id").toString());
            Float inputQty = Float.parseFloat(lotMap.get("cur_stock").toString());

            MaterialLot ml = this.matLotRepository.getMatLotById(lotId);
            validator.addLotInput_validateByMatLot(ml, "가용한 재고가 없는 LOT을 지정했습니다.(" + ml.getLotNumber() + ")",
                                                      "해당 품목의 기본창고가 지정되지 않았습니다(" + ml.getLotNumber() + ")");

            List<MatProcInput> mpiList = this.matProcInputRepository.findByMaterialProcessInputRequestIdAndMaterialLotId(jr.getMaterialProcessInputRequestId(), ml.getId());
            Integer mpiCount = mpiList.size();
            validator.throwIfExists(mpiCount, "이미 지정된 로트입니다.(" + ml.getLotNumber() + ")");

            MatProcInputReq mir = null;

            if (jr != null) {
                if (jr.getMaterialProcessInputRequestId() == null) {
                    mir = new MatProcInputReq();
                    mir.setRequestDate(inoutTime);
                    mir.setRequesterId(user.getId());
                    mir.set_audit(user);
                    mir = this.matProcInputReqRepository.save(mir);
                    jr.setMaterialProcessInputRequestId(mir.getId());

                } else {
                    mir = this.matProcInputReqRepository.getMatProcInputReqById(jr.getMaterialProcessInputRequestId());
                }
            }

            MatProcInput mpi = new MatProcInput();
            mpi.setMaterialProcessInputRequestId(mir.getId());
            mpi.setMaterialId(ml.getMaterialId());
            mpi.setRequestQty(inputQty);
            mpi.setInputQty((float) 0);
            mpi.setMaterialLotId(ml.getId());
            mpi.setMaterialStoreHouseId(ml.getStoreHouseId());
            mpi.setState("requested");
            mpi.setInputDateTime(inoutTime);
            mpi.setActorId(user.getId());
            mpi.set_audit(user);
            mpi = this.matProcInputRepository.save(mpi);

            result.success = true;
            result.data = mpi;
        }

        return result;
    }

    /**
     * 1. MaterialProduce의 size를 구해서 현재 차수 계산 (현재차수: size+1)
     * 2. lotPreFix 앞글자는 기본은 "B" , 만약 제품그룹이 product면 "P"
     * 3. make_production_lot_in_number메서드를 통해 LOT 번호 생성
     * ├─ 3-1. seq_maker 테이블을 조회하여 기존 Currval 값의 +1 한 값을 구한뒤, lotPreFix + "-" + "yyyyMMdd" + 0001  이런형식으로 로트번호 추출
     * 4. 차수 +1 하여 mat_rpdo 생성(MaterialProduce) 저장
     * 5. 생산품 mat_lot 생성하여 저장
     * 6. 차수생산량 만큼 good_qty량 만큼 BOM 수량조회
     * 7. BOM 만큼 반복문으로 차감로직 진행
     *
     * 8. LOT 관리 품목일 경우:
     * 투입된 로트 만큼 반복하여
     * MatLotCons 생성 (소비내역)
     * 					├─ currentStock 이 충분할 경우 : mlc에 출고량 삽입 후 저장, remainQty(남은투입량) 은 0
     * 					├─ currentStock 이 부족할 경우 : mlc에 출고량 삽입 후 저장
     * 즉 둘다 해당 로트를 소진시키는 작업이다. 둘다 mlc에 저장.
     *
     * 9. LOT 관리 품목이 아닐 경우: totalQty += chasuBomQty;
     *
     * 10. mat_cons 생성
     *
     * 11. mat_inout 생성=> lot 투입이면 투입 수량만큼 lot 없으면 BOM 수량만큼 재고를 차감한다.
     *
     * 12. bom_list 반복문을 빠져나와서 , mat_inout 생성=> 차수 수량만큼 재고를 증감한다.
     *
     * 13. mat_lot 의 출고량과 현재고 수량 업데이트
     *
     * 14. 작업지시의 양품량 합계 업데이트
     * **/
    @PostMapping("/chasu_add")
    @Transactional
    public AjaxResult chasuAdd(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "good_qty", required = false) Float goodQty,
            @RequestParam("spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        User user  = (User) auth.getPrincipal();

        AjaxResult result = productionResultService.chasu_add_service(jrPk, goodQty, spjangcd, user);

        return result;

        //TODO: mat_inout에 트리거 걸려있다 인지해라.
    }

    @PostMapping("/chasu_del")
    @Transactional
    public AjaxResult chasuDel(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestBody List<Map<String, Object>> chasuList,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        jrPk = Integer.parseInt(chasuList.get(0).get("jrPk").toString()); //그냥 아무 인덱스에서 뽑아와도됨 다 같은값

        User user = (User) auth.getPrincipal();

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        // mat_prod 마지막 차수 가져오기
        List<MaterialProduce> mpList = this.matProduceRepository.findByJobResponseIdAndLotNumberIn(jrPk, chasuList.stream().map(t -> (String) t.get("lot_no")).toList());
        if (mpList.isEmpty()) {
            throw new CustomException("차수생산이력이 존재하지 않습니다.");
        }

        List<Integer> LotIndexList = chasuList.stream()
                .map(t -> (Integer) t.get("chasu"))
                .toList();


        // mat_cons 가져오기
        List<MaterialConsume> mcList = this.matConsuRepository.findByJobResponseIdAndLotIndexIn(jr.getId(), LotIndexList);

        List<String> lotNumbers = mpList.stream().map(MaterialProduce::getLotNumber).toList();

        //생산된 차수 LOT의 mat_lot_consu 존재 확인
        List<MaterialLot> ml = this.matLotRepository.getByLotNumberIn(lotNumbers);

        List<Integer> lotIds = ml.stream().map(MaterialLot::getId).toList();

        //얘는 없어야함 --> 투입된 흔적이 없어야 삭제가능하므로
        List<MatProcInput> mpiList = this.matProcInputRepository.findByMaterialLotIdIn(lotIds); //TODO: check
        //얘도 없어야함 --> 생산한 완제품 lot가 소모 목록에 있으면 투입된 중이므로
        List<MatLotCons> mlcList = this.matLotConsRepository.findByMaterialLotIdIn(lotIds);  //TODO : check

        if(!mpiList.isEmpty()){
            throw new CustomException("생산LOT에 투입요청 중에 있는 LOT가 있어 삭제가 불가능합니다.");
        }
        //차수 생산으로 발행된 로트가 mat_lot_consu에 존재하는지
        if(!mlcList.isEmpty()){
            throw new CustomException("생산LOT에 투입요청 중에 있는 LOT가 있어 삭제가 불가능합니다.");
        }

        // mat_lot 삭제
        this.matLotRepository.deleteAllByIdIn(lotIds);

        // mat_lot_cons 삭제
        this.matLotConsRepository.deleteBySourceTableNameAndSourceDataPkIn("mat_produce", lotIds);

        // mat_inout 삭제
        List<Integer> matProduceIds = mpList.stream()
                .map(MaterialProduce::getId)
                .toList();
        this.matInoutRepository.deleteBySourceTableNameAndSourceDataPksAndInOutAndInputType("mat_produce", matProduceIds, "in", "produced_in");

        // 5.mat_inout 생산 재고 차감 이력 삭제 (자재원복), mat_cons 삭제
        for(int i=0; i < mcList.size(); i++){ //TODO: 이거 반복문 보다는 한번에 하는게 나을듯? 일단은 이렇게 하고
            this.matInoutRepository.deleteBySourceTableNameAndSourceDataPkAndInOutAndOutputType("mat_consu", mcList.get(i).getId(), "out", "consumed_out");
            this.matConsuRepository.deleteById(mcList.get(i).getId());
        }

        //6.해당 차수 mat_prod 삭제
        this.matProduceRepository.deleteByIdIn(mpList.stream().map(t -> t.getId()).toList());

        this.productionResultService.calculate_balance_mat_lot_with_job_res(jr.getId());

        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.productionResultService.getJobResponseGoodDefectQty(jrPk);

        float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
        float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());

        float removedGoodQty = mpList.stream().map(MaterialProduce::getGoodQty).filter(Objects::nonNull).reduce(0f, Float::sum);
        float removedDefectQty = mpList.stream().map(MaterialProduce::getDefectQty).filter(Objects::nonNull).reduce(0f, Float::sum);

        goodQtySum -= removedGoodQty;
        defectQtySum -= removedDefectQty;

        // 음수가 되지 않도록 보정
        if (goodQtySum < 0) goodQtySum = 0;
        if (defectQtySum < 0) defectQtySum = 0;

        jr.setGoodQty(goodQtySum);
        jr.setDefectQty(defectQtySum);
        jr.set_audit(user);
        jr = this.jobResRepository.save(jr);

        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_pk", jrPk);
        item.put("good_qty_sum", goodQtySum);
        item.put("defect_qty_sum", defectQtySum);

        result.data = item;
        return result;
    }

    @PostMapping("/chasu_save")

    public AjaxResult chasuSave(
            @RequestBody List<Map<String, Object>> chasuList,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        List<Map<String, Object>> resultList = new ArrayList<>();

        for (Map<String, Object> chasu : chasuList) {
            Integer jrPk = Integer.parseInt(chasu.get("jr_pk").toString());
            Integer mpId = Integer.parseInt(chasu.get("mp_id").toString());
            Float goodQty = Float.parseFloat(chasu.get("good_qty").toString());
            Float defectQty = Float.parseFloat(chasu.get("defect_qty").toString());

            AjaxResult singleResult = productionResultService.saveSingleChasu(jrPk, mpId, goodQty, defectQty, auth);

            resultList.add((Map<String, Object>) singleResult.data);
        }

        result.success = true;
        result.data = resultList;
        return result;
    }



    /**
     * 1. 생산량이 존재하면 삭제불가
     * 2. mat_consu (등록된 차수)가 있으면 삭제불가
     * 3. 부모프로젝트가
     *  ├─있는경우: jobresId + equipmentId 모두 일치하는 행만 EquRun에서 삭제 및 해당작업지시(job_res) 삭제
     * 	├─없는경우: 	설비가동내역(equ_run)에 "작지 취소" 로 인한 stop으로 상태변경후 해당 작지 삭제
     * ***/
    // 생산정보 삭제
    @PostMapping("/del")
    public AjaxResult prodResultDel(
            @RequestParam("id") Integer jobresId,
            @RequestParam(value = "order_num", required = false) String orderNum,
            @RequestParam(value = "equipment_id", required = false) Integer equipmentId,
            Authentication auth
    ) {

        User user = (User) auth.getPrincipal();

        productionResultService.JobResDel(jobresId, orderNum, equipmentId, user);
         //job_res_processTree 업데이트

        return AjaxResult.success("작업지시가 삭제되었습니다.", null);
    }

    /**
     * mat_proc_input : 제품 생산 투입내역에서 해당 lot 투입 삭제
     * */
    @PostMapping("/del_lot_list")
    @Transactional
    public AjaxResult delLotlist(
            @RequestParam(value = "mpi_pk", required = false) Integer mpi_pk,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        this.matProcInputRepository.deleteById(mpi_pk);
        return result;
    }

    //설비 가동 정보 조회
    @GetMapping("/readOrder")
    public AjaxResult getEquipmentdRunChart(
            @RequestParam(value = "WorkOrderNumber", required = false) String orderNum,
            HttpServletRequest request) {

        List<Map<String, Object>> items = this.equipmentService.getEquipmentOrderNum(orderNum);
        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    // 중지 시작 (재가동) 일시중지, 재가동
    /**
     * 1. equ_run 데이터 조작
     * 2. jobRes에서 동작 상태 변경
     * */
    @PostMapping("/stop_save")
    @Transactional
    public AjaxResult stopSave(
            @RequestParam(value = "stop_date", required = false) String stop_date,
            @RequestParam(value = "stopTime", required = false) String stopTime,
            @RequestParam(value = "WorkOrderNumber", required = false) String WorkOrderNumber,
            @RequestParam(value = "Description", required = false) String Description,
            @RequestParam(value = "Equipment_id", required = false) Integer Equipment_id,
            @RequestParam(value = "StopCause_id", required = false) Integer StopCause_id,
            @RequestParam(value = "jr_pk", required = false) Integer jr_pk,
            @RequestParam("spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();


        // 현재 시간의 초를 구함
        int currentSecond = LocalDateTime.now().getSecond();

        // stopTime (예: "10:32")에 초를 붙임 → "10:32:47"
        String fullStopTime = stopTime + ":" + String.format("%02d", currentSecond);

        // 최종 Timestamp 생성
        Timestamp stop_time = Timestamp.valueOf(stop_date + " " + fullStopTime);

        Timestamp now = DateUtil.getNowTimeStamp();

        Optional<EquRun> runningRunOpt = equRunRepository.findLatestRunningByEquipmentAndJobResId(Equipment_id, jr_pk);

        if (runningRunOpt.isPresent()) {
            EquRun equ = runningRunOpt.get();
            equ.setEndDate(stop_time); // 중지 시각
            equ.setRunState("stop");
            equ.setStopCauseId(StopCause_id);
            equ.setDescription(Description);
            equ.set_audit(user);
            equ.setSourceTableName("job_res");
            equ.setSourceDataPk(jr_pk);
            equ.setSpjangcd(spjangcd);

            equRunRepository.save(equ);

            jobResRepository.updateStateById(jr_pk, "stopped");
            return result;
        } else {



            long runningCount = equRunRepository.countByEquipmentIdAndRunState(Equipment_id, "run");


            if (runningCount > 0) {
                throw new CustomException("해당 설비는 이미 작업 중입니다. 재가동할 수 없습니다.");
            }

            EquRun er = new EquRun();
            er.setEquipmentId(Equipment_id);
            er.setStartDate(now);
            er.setWorkOrderNumber(WorkOrderNumber);
            er.setRunState("run");
            er.set_audit(user);
            er.setSourceTableName("job_res");
            er.setSourceDataPk(jr_pk);
            er.setSpjangcd(spjangcd);

            this.equRunRepository.save(er);

            jobResRepository.updateStateById(jr_pk, "working");

            result.message = "재개 되었습니다..";
            return result;
        }
    }


    @GetMapping("/prod_test_list")
    public AjaxResult prodTestList(
            @RequestParam("jr_pk") Integer jrPk) {

        List<TestResult> trList = this.testResultRepository.findBySourceTableNameAndSourceDataPk("job_res", jrPk);

        List<Map<String, Object>> items = null;
        Integer testMasterId = null;

        if (!trList.isEmpty()) {
            items = this.productionResultService.prodTestList(jrPk, trList.get(0).getId());
        } else {
            // 검사 유형이 등록된 경우 조회 (품목별 1개 강제)
            testMasterId = this.productionResultService.getTestMasterByItem(jrPk);

            if (testMasterId != null) {
                items = this.productionResultService.prodTestListByTestMaster(testMasterId);
            } else {
                // 검사 유형이 없으면 기본 리스트를 불러옴 (제품검사)
                items = this.productionResultService.prodTestDefaultList();
            }
        }

        Map<String, Object> item = new HashMap<>();
        AjaxResult result = new AjaxResult();

        if (items != null && !items.isEmpty()) {
            item.put("testDate", items.get(0).get("testDate"));
            item.put("CheckName", items.get(0).get("CheckName"));
            item.put("JudgeCode", items.get(0).get("JudgeCode"));
            item.put("ctRemark", items.get(0).get("ctRemark"));
            item.put("ntRemark", items.get(0).get("ntRemark"));
            item.put("testMasterId", items.get(0).get("testMasterId"));
            item.put("testResultId", items.get(0).get("testResultId"));
            item.put("pdList", items);
        } else {
            // 안전하게 빈 리스트 전달
            item.put("pdList", new ArrayList<>());
            result.message = "검사 항목이 존재하지 않습니다.";
        }


        result.data = item;
        return result;
    }

    @PostMapping("/test_save")
    @Transactional
    public AjaxResult testSave(
            @RequestBody MultiValueMap<String, Object> Q,
            @RequestParam(value = "material_id", required = false) Integer materialId,
            @RequestParam(value = "ctRemark", required = false) String ctRemark,
            @RequestParam(value = "ntRemark", required = false) String ntRemark,
            @RequestParam(value = "test_mast_id", required = false) String testMastId,
            @RequestParam(value = "test_result_id", required = false) String testResultId,
            @RequestParam(value = "judg_grp", required = false) String judgGrp,
            @RequestParam(value = "test_date", required = false) String test_date,
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            HttpServletRequest request,
            Authentication auth) {

        User user = (User) auth.getPrincipal();

        AjaxResult result = new AjaxResult();

        Timestamp testDate = Timestamp.valueOf(test_date + " 00:00:00");

        if (StringUtils.hasText(testResultId)) {
            List<TestItemResult> trList = this.testItemResultRepository.findByTestResultId(Integer.parseInt(testResultId));

            // 결과 삭제
            if (trList.size() > 0) {
                for (int i = 0; i < trList.size(); i++) {
                    this.testItemResultRepository.deleteById(trList.get(i).getId());
                }
            }

            this.testItemResultRepository.flush();

        }

        TestResult tr = new TestResult();

        if (StringUtils.hasText(testResultId)) {
            tr = this.testResultRepository.getTestResultById(Integer.parseInt(testResultId));
        } else {
            tr.setSourceDataPk(jrPk);
            tr.setSourceTableName("job_res");
            tr.setMaterialId(materialId);
        }

        tr.setTestMasterId(Integer.parseInt(testMastId));
        tr.setTestDateTime(testDate);
        tr.set_audit(user);

        this.testResultRepository.saveAndFlush(tr);

        List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

        for (int i = 0; i < data.size(); i++) {
            TestItemResult tir = new TestItemResult();
            tir.setJudgeCode(judgGrp);
            tir.setTestDateTime(testDate);
            tir.setInputResult(ctRemark);
            tir.setCharResult(ntRemark);
            tir.setTestItemId(Integer.parseInt(data.get(i).get("id").toString()));
            tir.setTestResultId(tr.getId());

            if (data.get(i).get("result1") != null) {
                tir.setChar1(data.get(i).get("result1").toString());
            }

            if (data.get(i).get("result2") != null) {
                tir.setChar2(data.get(i).get("result2").toString());
            }
            tir.set_audit(user);

            this.testItemResultRepository.save(tir);
        }


        Map<String, Object> item = new HashMap<>();
        item.put("id", jrPk);

        result.data = item;

        return result;
    }
}
