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

import mes.Exception.CustomException;
import mes.app.definition.service.EquipmentService;
import mes.app.production.ProductuibResult_validation.ProductionResultValidator;
import mes.app.production.service.EquipmentRunChartService;
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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    private LotService lotService;

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
    WorkcenterRepository workcenterRepository;

    @Autowired
    StorehouseRepository storehouseRepository;

    @Autowired
    SystemOptionRepository systemOptionRepository;

    @Autowired
    MatLotRepository matLotRepository;

    @Autowired
    MatProcInputRepository matProcInputRepository;

    @Autowired
    MaterialGroupRepository materialGroupRepository;

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
    @Transactional
    public AjaxResult workFinish(
            @RequestParam(value = "id", required = false) Integer jrPk,
            @RequestParam(value = "lot_num", required = false) String lotNum,
            @RequestParam(value = "order_qty", required = false) Float orderQty,
            @RequestParam(value = "good_qty", required = false) Float goodQty,
            @RequestParam(value = "defect_qty", required = false) Float defectQty,
            @RequestParam(value = "loss_qty", required = false) Float lossQty,
            @RequestParam(value = "scrap_qty", required = false) Float scrapQty,
            @RequestParam(value = "shift_code", required = false) String shiftCode,
            @RequestParam(value = "mat_pk", required = false) Integer materialId,
            @RequestParam(value = "prod_mat_id", required = false) Integer prod_mat_id,
            @RequestParam(value = "workcenter_id", required = false) Integer workcenterId,
            @RequestParam(value = "equipment_id", required = false) Integer equipmentId,
            @RequestParam(value = "prod_date", required = false) String prodDate,
            @RequestParam(value = "start_time", required = false) String startTime,   // ← 전달만 받되, DB의 startTime 우선
            @RequestParam(value = "end_date", required = false) String endDate,
            @RequestParam(value = "end_time", required = false) String endTime,
            @RequestParam(value = "description", required = false) String description,
            @RequestParam(value = "order_num", required = false) String orderNum,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();


        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        validator.validateJobResExists(jr, "작업지를 찾을 수 없습니다."); //작업지시 존재하는지 체크
        validator.workFinish_Exists_endDate(endDate, endTime, "종료일/종료시간이 필요합니다."); //종료일자 및 종료시간 있는지 체크

        // 1) 포맷터
        DateTimeFormatter dtm = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

        // 2) DB에 저장된 startTime을 기준으로 초(second) 확보 (없으면 0)
        int sec = 0;
        if (jr.getStartTime() != null) {
            sec = jr.getStartTime().toLocalDateTime().getSecond();
        }

        // 3) end_time = end_date + end_time (+ sec)

        LocalDateTime endDt = LocalDateTime.parse(endDate + " " + endTime, dtm).withSecond(sec);
        Timestamp end_time = Timestamp.valueOf(endDt);

        // 4) startDt : DB값 우선. (없다면 form 값으로 보정, 그래도 없으면 오류)
        LocalDateTime startDt = null;
        if (jr.getStartTime() != null) {
            startDt = jr.getStartTime().toLocalDateTime();
        } else if (prodDate != null && !prodDate.isBlank() && startTime != null && !startTime.isBlank()) {
            startDt = LocalDateTime.parse(prodDate + " " + startTime, dtm).withSecond(sec);
        } else {
            throw new CustomException("시작시간이 없습니다. (작업시작 후 완료해주세요)");
        }

        // 5) 백엔드에서도 시간 역전 검증
        validator.reverseTimeValidator(endDt, startDt, "작업시간이 잘못되었습니다. (종료 < 시작)");

        // 6) 생산/차수/투입 체크
        List<MaterialConsume> mcList = this.matConsuRepository.findByJobResponseId(jrPk);
        validator.assertNotEmpty(mcList, "저장된 투입내역이 없습니다.\n투입내역을 저장해주세요.");

        List<MaterialProduce> mp = this.matProduceRepository.findByJobResponseId(jrPk);
        validator.assertNotEmpty(mp, "저장된 차수내역이 없습니다.\n차수내역을 저장해주세요.");

        // 7) JR 업데이트 (start_time 은 건드리지 않음!)
        Timestamp prod_date = CommonUtil.tryTimestamp(prodDate);
        jr.set_audit(user);
        jr.setLotNumber(lotNum);
        jr.setGoodQty(goodQty);
        jr.setDefectQty(defectQty);
        jr.setLossQty(lossQty);
        jr.setScrapQty(scrapQty);
        jr.setProductionDate(prod_date);
        jr.setEndDate(Date.valueOf(endDate));
        // jr.setStartTime(...)  ← 제거!
        jr.setEndTime(end_time);
        jr.setShiftCode(shiftCode);
        jr.setWorkCenter_id(workcenterId);
        jr.setEquipment_id(equipmentId);
        jr.setDescription(description);
        jr.setState("finished");

        //불량창고에 불량품 등록
        this.productionResultService.add_jobres_defectqty_inout(jrPk, user.getId());
        jr = this.jobResRepository.save(jr);

        // 8) EquRun 종료 (가능하면 jr_pk로 귀속하여 조회/종료 권장)
        Optional<EquRun> runningRunOpt =
                equRunRepository.findLatestRunningByEquipmentAndOrder(equipmentId, orderNum);
        if (runningRunOpt.isPresent()) {
            EquRun equ = runningRunOpt.get();
            equ.setEndDate(end_time);
            equ.setRunState("complete");
            equ.set_audit(user);
            equRunRepository.save(equ);
        }

        Map<String, Object> item = new HashMap<>();
        item.put("jr_pk", jrPk);
        result.success = true;
        result.data = item;
        return result;
    }

    /**
     * 1. 작업지시의 entTime = null , State는 working
     * 2. mat_inout에서 불량품으로 등록된 것들 삭제
     * 3. 설비동작 이력을 가져와서 RunState를 "complete_cancel" 로 수정후 새로운 행을 하나 추가(RunState : run)
     * */
    @PostMapping("/finish_cancel")
    @Transactional
    public AjaxResult finishCancel(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "spjangcd", required = false) String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        jr.setEndTime(null);
        jr.setState("working");
        jr.set_audit(user);

        jr = this.jobResRepository.save(jr);

        this.productionResultService.delete_jobres_defectqty_inout(jrPk);

        Optional<EquRun> latestComplete = equRunRepository.findLatestCompleteByEquipmentAndOrder(
                jr.getEquipment_id(), jr.getWorkOrderNumber());

        if (latestComplete.isPresent()) {
            EquRun equ = latestComplete.get();
            equ.setRunState("complete_cancel");
            equ.set_audit(user);
            equ.setDescription("완료 취소");
            equ.setSpjangcd(spjangcd);
            equRunRepository.save(equ);

            Timestamp nowWithCurrentSecond = Timestamp.valueOf(LocalDateTime.now());


            // 그리고 새로운 run 상태로 재시작
            EquRun newRun = new EquRun();
            newRun.setEquipmentId(jr.getEquipment_id());
            newRun.setWorkOrderNumber(jr.getWorkOrderNumber());
            newRun.setStartDate(nowWithCurrentSecond);
            newRun.setRunState("run");
            newRun.setSpjangcd(spjangcd);
            newRun.set_audit(user);

            equRunRepository.save(newRun);
        }

        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_pk", jrPk);

        result.success = true;
        result.data = item;

        return result;

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
        List<Map<String, Object>> bomMatItems = this.productionResultService.get_chasu_bom_mat_qty_list(mpEntityList.get(0).getId());
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

                List<Map<String, Object>> mpiList = this.productionResultService.getMaterialProcessInputList(jr.getId(), consumeMatPk);
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
            this.productionResultService.calculate_balance_mat_lot_with_job_res(jr.getId()); //TODO: 성능개선포인트
        }

        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.productionResultService.getJobResponseGoodDefectQty(jrPk);

        float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
        float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());
        jr.setGoodQty(goodQtySum);
        jr.setDefectQty(defectQtySum);
        jr.set_audit(user);

        jr = this.jobResRepository.save(jr);

        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_pk", jrPk);
        //item.put("lot_number", lotNumber);
        item.put("good_qty_sum", jr.getGoodQty());
        //item.put("chasu", chasu);
        item.put("prod_mat_cd", m.getCode());

        result.data = item;

        return result;

        //TODO: mat_inout에 트리거 걸려있다 인지해라.
    }

    //원본
    /*@PostMapping("/chasu_add")
    @Transactional
    public AjaxResult chasuAdd(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestParam(value = "good_qty", required = false) float goodQty,
            @RequestParam("spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) {

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
        validator.throwIfNull(jr, "작업지시가 존재하지 않습니다.");
        validator.throwIfNull(jr.getWorkCenter_id(), "워크센터가 지정되지 않았습니다.");

        Material m = this.materialRepository.getMaterialById(jr.getMaterialId());

        validator.throwIfNull(m, "해당 작업지시에 대한 품목정보가 없습니다.");
        validator.throwIfNull(m.getStoreHouseId(), "생산제품의 기본 창고가 설정되어 있지 않습니다.");

        Integer storehouseId = m.getStoreHouseId();

        // 작업지시량 / lot당 수량
        int totalLotCnt = (int) (jr.getOrderQty() / goodQty);


        // matprods 개수로
        List<MaterialProduce> mpList = this.matProduceRepository.findByJobResponseId(jr.getId());
        Integer startChasu = mpList.size() + 1;
        Integer chasu = mpList.size() + 1;
        List<Integer> lotChasuList = new ArrayList<>();
        for(int i= startChasu; i <= totalLotCnt; i++){
            lotChasuList.add(i);
        }


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

        String lotNumber = this.lotService.make_production_lot_in_number(lotPrefix);

        // 차수별 mat_produce
        MaterialProduce mp = new MaterialProduce();
        mp.setJobResponseId(jr.getId());
        mp.setMaterialId(m.getId());
        mp.setProcessId(processId);
        mp.setProcessOrder(1);
        mp.setLotIndex(chasu);
        mp.setState("finished");
        mp.set_status("a");
        mp.setStoreHouseId(storehouseId); // 공정창고 or 제품창고
        mp.setProductionDate(jr.getProductionDate());
        mp.setStartTime(jr.getStartTime());
        mp.setEndTime(now);
        mp.setShiftCode(jr.getShiftCode());
        mp.setWorkCenterId(jr.getWorkCenter_id());
        mp.setEquipmentId(jr.getEquipment_id());
        mp.setGoodQty((float) goodQty);
        mp.setDescription("차수생산");
        mp.setActorId(user.getId());
        mp.set_audit(user);
        mp.setLastProcessYN("Y");
        mp.setLotNumber(lotNumber);
        mp.setSpjangcd(spjangcd);
        mp = this.matProduceRepository.save(mp); // mat_prod 생성

        // 2.생산품 mat_lot 생성
        MaterialLot ml = new MaterialLot();
        ml.setLotNumber(lotNumber);
        ml.setMaterialId(m.getId());
        ml.setInputDateTime(now);
        ml.setInputQty(mp.getGoodQty());
        ml.setCurrentStock(mp.getGoodQty());
        ml.setDescription(chasu + "차수생산");
        ml.setSourceDataPk(mp.getId());
        ml.setSourceTableName("mat_produce");
        ml.setStoreHouseId(mp.getStoreHouseId());
        ml.set_audit(user);
        ml.setSpjangcd(spjangcd);
        ml = this.matLotRepository.save(ml); // materialLot 저장

        // 차수생산량 만큼 good_qty량 만큼 BOM 수량조회
        List<Map<String, Object>> bomMatItems = this.productionResultService.get_chasu_bom_mat_qty_list(mp.getId());
        validator.assertNotEmpty(bomMatItems, "BOM구성이 없습니다.");


        for (int i = 0; i < bomMatItems.size(); i++) {
            Map<String, Object> bomMap = bomMatItems.get(i);

            float chasuBomQty = Float.parseFloat(bomMap.get("chasu_bom_qty").toString());

            int consumeMatPk = (int) bomMap.get("mat_pk");
            String matName = bomMap.get("mat_name").toString();
            Material consMat = this.materialRepository.getMaterialById(consumeMatPk);
            String lotUseYn = bomMap.get("lotUseYn").toString();
            float totalQty = 0f;

			 *//*선입선출로 mat_lot 찾아서 차감
             차감하면서 mat_lot_cons 생성
             투입되어야할 수량보다 적으면 재고량 부족으로 return*//*


            if ("Y".equals(lotUseYn)) { //lot 관리를 할 경우
                // 수정시작
                // 1. mat_proc_input 에서 해당 품목의 로트리스트를 가져온다.

                List<Map<String, Object>> mpiList = this.productionResultService.getMaterialProcessInputList(jr.getId(), consumeMatPk);
                // 투입요청에서 해당 품목이 로트 투입인지 조회한다

                float remainQty = chasuBomQty; //남은 투입량

                for (int j = 0; j < mpiList.size(); j++) {
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

                }

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
                totalQty += chasuBomQty;
            }

            // mat_cons 생성
            MaterialConsume mc = new MaterialConsume();
            mc.setJobResponseId(jr.getId());
            mc.setMaterialId(consumeMatPk);
            mc.setProcessOrder(mp.getProcessOrder());
            mc.setLotIndex(mp.getLotIndex());
            mc.setStartTime(now);
            mc.setEndTime(now);
            mc.setDescription("차수생산분");
            mc.setBomQty(chasuBomQty);
            mc.setConsumedQty(totalQty);        // 차수 생산분에 해당하는 BOM기준물량, lot 사용시 총 투입 수량
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
            mic.setLotNumber(mp.getLotNumber());
            mic.setInoutDate(LocalDate.parse(date.format(dateFormat)));
            mic.setInoutTime(LocalTime.parse(time.format(timeFormat)));
            mic.setInOut("out");
            mic.setOutputType("consumed_out");
            mic.setOutputQty(totalQty);
            mic.setSourceDataPk(mc.getId());
            mic.setSourceTableName("mat_consu");
            mic.setState("confirmed");
            mic.set_status("a");
            mic.setDescription("차수생산 투입재고 차감");
            mic.set_audit(user);
            mic.setSpjangcd(spjangcd);

            mic = this.matInoutRepository.save(mic);
        } // bom List 반목문 끝, for문 끝

        // 2. mat_inout 생성=> 차수 수량만큼 재고를 증감한다.
        MaterialInout mip = new MaterialInout();
        mip.setMaterialInoutHeadId(null);
        mip.setMaterialId(m.getId());
        mip.setStoreHouseId(m.getStoreHouseId());
        mip.setLotNumber(mp.getLotNumber());
        mip.setInoutDate(LocalDate.parse(date.format(dateFormat)));
        mip.setInoutTime(LocalTime.parse(time.format(timeFormat)));
        mip.setInOut("in");
        mip.setInputQty(mp.getGoodQty());
        mip.setInputType("produced_in");
        mip.setSourceDataPk(mp.getId());
        mip.setSourceTableName("mat_produce");
        mip.setState("confirmed");
        mip.set_status("a");
        mip.setDescription("차수생산입고");
        mip.set_audit(user);
        mip.setSpjangcd(spjangcd);

        mip = this.matInoutRepository.save(mip);

        //mat_lot 의 출고량과 현재고 수량 업데이트
        this.productionResultService.calculate_balance_mat_lot_with_job_res(jr.getId());

        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.productionResultService.getJobResponseGoodDefectQty(jrPk);

        float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
        float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());
        jr.setGoodQty(goodQtySum);
        jr.setDefectQty(defectQtySum);
        jr.set_audit(user);

        jr = this.jobResRepository.save(jr);

        Map<String, Object> item = new HashMap<String, Object>();
        item.put("jr_pk", jrPk);
        item.put("lot_number", lotNumber);
        item.put("good_qty_sum", jr.getGoodQty());
        item.put("chasu", chasu);
        item.put("prod_mat_cd", m.getCode());

        result.data = item;

        return result;
    }*/

    @PostMapping("/chasu_del")
    @Transactional
    public AjaxResult chasuDel(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            @RequestBody List<Map<String, Object>> chasuList,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        jrPk = null; //TODO

        if(1==1) throw new RuntimeException("asd");

        User user = (User) auth.getPrincipal();

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        // mat_prod 마지막 차수 가져오기
        List<MaterialProduce> mpList = this.matProduceRepository.findByJobResponseIdOrderByLotIndexDesc(jrPk);
        Integer matProdCount = mpList.size();

        if (matProdCount == 0) {
            throw new CustomException("차수생산이력이 존재하지 않습니다.");
        }

        /*for(int i=0; i < chasuList.size(); i++){

            //Lotindex 리스트
            chasuList.stream().filter()

            MaterialProduce mp = mpList.get(i);
            String lotNumber = mp.getLotNumber();
            float removedGoodQty = (mp.getGoodQty() != null) ? mp.getGoodQty() : 0;
            float removedDefectQty = (mp.getDefectQty() != null) ? mp.getDefectQty() : 0;

            List<MaterialConsume> mcList = this.matConsuRepository.findByJobResponseIdAndLotIndexIn(jr.getId(), chasuList);
        }*/
        MaterialProduce mp = mpList.get(0);
        String lotNumber = mp.getLotNumber();
        float removedGoodQty = (mp.getGoodQty() != null) ? mp.getGoodQty() : 0;
        float removedDefectQty = (mp.getDefectQty() != null) ? mp.getDefectQty() : 0;


        // mat_cons 가져오기
        List<MaterialConsume> mcList = this.matConsuRepository.findByJobResponseIdAndProcessOrderAndLotIndex(jr.getId(), mp.getProcessOrder(), mp.getLotIndex());

        // Integer matConsumeCount = mcList.size();

        // 생산된차수LOT의 mat_lot_consu 존재 확인
        MaterialLot ml = this.matLotRepository.getByLotNumber(lotNumber);

        List<MatProcInput> mpiList = this.matProcInputRepository.findByMaterialLotId(ml.getId());

        List<MatLotCons> mlcList = this.matLotConsRepository.findByMaterialLotId(ml.getId());

        if (mpiList.size() > 0) {
            result.message = "생산LOT(" + lotNumber + ")이 투입요청 중에 있어 차수 삭제가 불가능합니다.";
            result.success = false;
            return result;
        }
        // 차수 생산으로 발행된 로트가 mat_lot_consu에 존재하는지
        if (mlcList.size() > 0) {
            // 1. 생산된 차수의 생산로트가 다론곳에서 사용되었으면 돌이킬 수 없다.
            result.message = "생산LOT(" + lotNumber + ")이 사용중에 있어 차수 삭제가 불가능합니다.";
            result.success = false;
            return result;
        }

        // 2. mat_lot 삭제
        this.matLotRepository.deleteById(ml.getId());

        // mat_lot_cons 삭제
        this.matLotConsRepository.deleteBySourceTableNameAndSourceDataPk("mat_produce", mp.getId());

        // mat_inout 삭제
        this.matInoutRepository.deleteBySourceTableNameAndSourceDataPkAndInOutAndInputType("mat_produce", mp.getId(), "in", "produced_in");

        // 5. mat_inout 생산 재고 차감 이력 삭제 (재고원복), mat_cons삭제
        // mat_cons 삭제(투입 자재별로 등록된 mat_consu)
        for (int i = 0; i < mcList.size(); i++) {
            this.matInoutRepository.deleteBySourceTableNameAndSourceDataPkAndInOutAndOutputType("mat_consu", mcList.get(i).getId(), "out", "consumed_out");
            this.matConsuRepository.deleteById(mcList.get(i).getId());
        }

        // 6.해당 차수 mat_prod 삭제
        this.matProduceRepository.deleteById(mp.getId());

        this.productionResultService.calculate_balance_mat_lot_with_job_res(jr.getId());

        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.productionResultService.getJobResponseGoodDefectQty(jrPk);

        float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
        float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());

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

    /*@PostMapping("/chasu_del")
    @Transactional
    public AjaxResult chasuDel(
            @RequestParam(value = "jr_pk", required = false) Integer jrPk,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User) auth.getPrincipal();

        JobRes jr = this.jobResRepository.getJobResById(jrPk);

        // mat_prod 마지막 차수 가져오기
        List<MaterialProduce> mpList = this.matProduceRepository.findByJobResponseIdOrderByLotIndexDesc(jrPk);
        Integer matProdCount = mpList.size();

        if (matProdCount == 0) {
            result.message = "차수생산이력이 존재하지 않습니다.";
            result.success = false;
            return result;
        }

        MaterialProduce mp = mpList.get(0);
        String lotNumber = mp.getLotNumber();
        float removedGoodQty = (mp.getGoodQty() != null) ? mp.getGoodQty() : 0;
        float removedDefectQty = (mp.getDefectQty() != null) ? mp.getDefectQty() : 0;


        // mat_cons 가져오기
        List<MaterialConsume> mcList = this.matConsuRepository.findByJobResponseIdAndProcessOrderAndLotIndex(jr.getId(), mp.getProcessOrder(), mp.getLotIndex());
        // Integer matConsumeCount = mcList.size();

        // 생산된차수LOT의 mat_lot_consu 존재 확인
        MaterialLot ml = this.matLotRepository.getByLotNumber(lotNumber);

        List<MatProcInput> mpiList = this.matProcInputRepository.findByMaterialLotId(ml.getId());

        List<MatLotCons> mlcList = this.matLotConsRepository.findByMaterialLotId(ml.getId());

        if (mpiList.size() > 0) {
            result.message = "생산LOT(" + lotNumber + ")이 투입요청 중에 있어 차수 삭제가 불가능합니다.";
            result.success = false;
            return result;
        }
        // 차수 생산으로 발행된 로트가 mat_lot_consu에 존재하는지
        if (mlcList.size() > 0) {
            // 1. 생산된 차수의 생산로트가 다론곳에서 사용되었으면 돌이킬 수 없다.
            result.message = "생산LOT(" + lotNumber + ")이 사용중에 있어 차수 삭제가 불가능합니다.";
            result.success = false;
            return result;
        }

        // 2. mat_lot 삭제
        this.matLotRepository.deleteById(ml.getId());

        // mat_lot_cons 삭제
        this.matLotConsRepository.deleteBySourceTableNameAndSourceDataPk("mat_produce", mp.getId());

        // mat_inout 삭제
        this.matInoutRepository.deleteBySourceTableNameAndSourceDataPkAndInOutAndInputType("mat_produce", mp.getId(), "in", "produced_in");

        // 5. mat_inout 생산 재고 차감 이력 삭제 (재고원복), mat_cons삭제
        // mat_cons 삭제(투입 자재별로 등록된 mat_consu)
        for (int i = 0; i < mcList.size(); i++) {
            this.matInoutRepository.deleteBySourceTableNameAndSourceDataPkAndInOutAndOutputType("mat_consu", mcList.get(i).getId(), "out", "consumed_out");
            this.matConsuRepository.deleteById(mcList.get(i).getId());
        }

        // 6.해당 차수 mat_prod 삭제
        this.matProduceRepository.deleteById(mp.getId());

        this.productionResultService.calculate_balance_mat_lot_with_job_res(jr.getId());

        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.productionResultService.getJobResponseGoodDefectQty(jrPk);

        float goodQtySum = Float.parseFloat(mapSum.get("good_qty").toString());
        float defectQtySum = Float.parseFloat(mapSum.get("defect_qty").toString());

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
    }*/

    @PostMapping("/chasu_save")
    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
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

            AjaxResult singleResult = saveSingleChasu(jrPk, mpId, goodQty, defectQty, auth);

            if (!singleResult.success) {
                // 실패 시 롤백 + 에러 메시지 반환
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                return singleResult;
            }

            resultList.add((Map<String, Object>) singleResult.data);
        }

        result.success = true;
        result.data = resultList;
        return result;
    }

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

        if (prodMatLotConsCount.size() > 0) {
            result.message = "해당차수의 로트가 이미 사용되어 수정할 수 없습니다.";
            result.success = false;
            return result;
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
            Map<String, Object> mapSum = this.productionResultService.getJobResponseGoodDefectQty(jrPk);

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
        List<Map<String, Object>> bomMatItems = this.productionResultService.get_chasu_bom_mat_qty_list(mp.getId());

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

                List<Map<String, Object>> mpiList = this.productionResultService.getMaterialProcessInputList(jr.getId(), consumeMatPk);
                // 투입요청에서 해당 품목이 로트 투입인지 조회한다

                float totalLotQty = 0;
                for (int j = 0; j < mpiList.size(); j++) {
                    Map<String, Object> mpiMap = mpiList.get(j);

                    float currQty = Float.parseFloat(mpiMap.get("curr_qty").toString());
                    totalLotQty += currQty;
                }

                if (totalLotQty < chasuBomQty) {
                    result.message = "가용한 LOT 재고가 없습니다.(" + matName + ")\n 투입 내역에서 가용 재고를 추가해주세요. ";
                    result.success = false;
                    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                    return result;
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
                    result.message = "사용 불가능한 품목이 BOM에 등록되어 있습니다.(" + matName + ")";
                    result.success = false;
                    TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                    return result;
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
        this.productionResultService.calculate_balance_mat_lot_with_job_res(jr.getId());
        // 양품량 합계 업데이트
        Map<String, Object> mapSum = this.productionResultService.getJobResponseGoodDefectQty(jrPk);

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

    /**
     * 1. 생산량이 존재하면 삭제불가
     * 2. mat_consu (등록된 차수)가 있으면 삭제불가
     * 3. 부모프로젝트가
     *  ├─있는경우: jobresId + equipmentId 모두 일치하는 행만 EquRun에서 삭제 및 해당작업지시(job_res) 삭제
     * 	├─없는경우: 	설비가동내역(equ_run)에 "작지 취소" 로 인한 stop으로 상태변경후 해당 작지 삭제
     * ***/
    // 생산정보 삭제
    @PostMapping("/del")
    @Transactional
    public AjaxResult prodResultDel(
            @RequestParam("id") Integer jobresId,
            @RequestParam(value = "order_num", required = false) String orderNum,
            @RequestParam(value = "equipment_id", required = false) Integer equipmentId,
            Authentication auth
    ) {
        AjaxResult result = new AjaxResult();

        // 대상 작업지
        JobRes jr = jobResRepository.getJobResById(jobresId);
        validator.validateJobResExists(jr,"대상 작업지시를 찾을 수 없습니다.");


        // 1) 생산량 가드: 양품+불량 > 0 이면 취소/삭제 불가
        double good = jr.getGoodQty() == null ? 0d : jr.getGoodQty().doubleValue();
        double defect = jr.getDefectQty() == null ? 0d : jr.getDefectQty().doubleValue();
        if (good + defect > 0) {
            throw new CustomException("생산량이 존재하여 삭제할 수 없습니다.");
        }

        // (선택) 차수/투입 등 존재 시 가드 유지
        // 기존 코드 유지: 등록된 차수(=소모/투입 등) 있으면 삭제 불가
        List<MaterialConsume> mcList = matConsuRepository.findByJobResponseId(jobresId);
        if (mcList != null && !mcList.isEmpty()) {
            throw new CustomException("등록된 차수가 있어 삭제할 수 없습니다.");
        }

        User user = (User) auth.getPrincipal();
        Timestamp now = DateUtil.getNowTimeStamp();

        boolean isChild = jr.getParentId() != null; // ← 프로젝트 필드명에 맞게 변경

        if (isChild) {
            if (equipmentId == null) {
                throw new CustomException("장비 정보가 없어 삭제할 수 없습니다.");
            }

            // EquRun: jobresId + equipmentId 모두 일치하는 행만 삭제
            int deleted = equRunRepository.deleteByWorkOrderNumberAndEquipmentId(orderNum, equipmentId);
            // 필요시 로그: log.info("EquRun deleted rows: {}", deleted);

            // 자식 JobRes 삭제
            jobResRepository.deleteById(jobresId);

            result.success = true;
            result.message = "자식 작업지와 관련 설비가동 기록이 삭제되었습니다.";
            return result;
        }
        else {
            // ======================
            // PLAN(부모) 취소 플로우
            // ======================

            // 진행 중인 설비가동은 stop 처리 (기존 로직 유지)
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

            //자식&부모 작지 삭제
            List<JobRes> child_list = jobResRepository.findBySourceDataPkAndSourceTableName(jr.getSourceDataPk(), jr.getSourceTableName());

            jobResRepository.deleteAll(child_list);



            result.success = true;
            result.message = "작업지시가 삭제되었습니다.";
            return result;
        }
    }

    // 생산정보 삭제
//	@PostMapping("/del")
//	@Transactional
//	public AjaxResult prodResultDel(
//			@RequestParam(value="id" , required=false) Integer jobresId,
//			HttpServletRequest request,
//			Authentication auth) {

//		AjaxResult result = new AjaxResult();
//
//		JobRes jr = this.jobResRepository.getJobResById(jobresId);
//
//		User user = (User)auth.getPrincipal();
//
//		List<MaterialProduce> mpList =  new ArrayList<>();
//		List<JobResDefect> jdList =  new ArrayList<>();
//
//		if (!jr.getState().equals("finisehed") && !jr.getSourceTableName().equals("suju")) {
//
//			mpList = this.matProduceRepository.findByJobResponseId(jr.getId());
//			jdList = this.jobResDefectRepository.findByJobResponseId(jr.getId());
//
//			if (mpList.size() > 0) {
//				result.success = false;
//				result.message = "저장된 차수가 존재합니다.";
//				return result;
//			}
//		}
//		String state = "";
//
//		if (jr.getSourceTableName().equals("suju")) {
//			Suju s = this.sujuRepository.getSujuById(jr.getSourceDataPk());
//			boolean jrExist = false;
//
//			if (s.getMaterialId() == jr.getMaterialId()) {
//				List<JobRes> jrList = this.jobResRepository.findBySourceDataPkAndSourceTableName(s.getId(),"suju");
//
//				// 로직 맞는지 점검
//				for (int i = 0; i < jrList.size(); i++) {
//					Material m = this.materialRepository.getMaterialById(jrList.get(i).getMaterialId());
//					MaterialGroup mg = this.materialGroupRepository.getMatGrpById(m.getMaterialGroupId());
//
//					if (jrList.get(i).getId() == jr.getId() || mg.getMaterialType().equals("product")) {
//						jrList.remove(i);
//					}
//				}
//
//				if (jrList.size() > 0) {
//					jrExist = true;
//				}
//
//				if(jrExist) {
//					result.message = "반제품 작업지시가 존재합니다.\\n반제품 작지를 삭제해주세요.";
//					result.success = false;
//					return result;
//				} else {
//
//					List<Integer> id = new ArrayList<Integer>();
//					id.add(jr.getId());
//
//					jrList = this.jobResRepository.findBySourceDataPkAndSourceTableNameAndMaterialIdAndIdNotIn(s.getId(),"suju",jr.getMaterialId(),id);
//
//					if (jrList.size() == 0 ) {
//						state = "received";
//					}
//				}
//			}
//		}
//
//		Integer sujuPk = jr.getSourceDataPk();
//
//		if (jdList.size() > 0) {
//			for (int i = 0; i < jdList.size(); i++) {
//				this.jobResDefectRepository.deleteById(jdList.get(i).getId());
//			}
//		}
//
//		if (state.equals("received")) {
//			Suju sj = this.sujuRepository.findByIdAndState(sujuPk,"ordered");
//
//			if (sj != null) {
//				sj.setState("received");
//				sj.set_audit(user);
//				sj = this.sujuRepository.save(sj);
//			}
//
//			this.jobResRepository.deleteById(jr.getId());
//		}
//
//
//		return result;
//	}

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

    // 중지 시작 (재가동)
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

        Optional<EquRun> runningRunOpt = equRunRepository.findLatestRunningByEquipmentAndOrder(Equipment_id, WorkOrderNumber);

        if (runningRunOpt.isPresent()) {
            EquRun equ = runningRunOpt.get();
            equ.setEndDate(stop_time); // 중지 시각
            equ.setRunState("stop");
            equ.setStopCauseId(StopCause_id);
            equ.setDescription(Description);
            equ.set_audit(user);
            equ.setSpjangcd(spjangcd);

            equRunRepository.save(equ);

            jobResRepository.updateStateById(jr_pk, "stopped");
            return result;
        } else {
            long runningCount = equRunRepository.countByEquipmentIdAndRunState(Equipment_id, "run");
            if (runningCount > 0) {
                result.success = false;
                result.message = "해당 설비는 이미 작업 중입니다. 재가동할 수 없습니다.";
                return result;
            }

            EquRun er = new EquRun();
            er.setEquipmentId(Equipment_id);
            er.setStartDate(now);
            er.setWorkOrderNumber(WorkOrderNumber);
            er.setRunState("run");
            er.set_audit(user);
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
