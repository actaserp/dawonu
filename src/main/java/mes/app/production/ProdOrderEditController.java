package mes.app.production;

import java.awt.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import mes.Exception.CustomException;
import mes.app.common.NotificationController;
import mes.app.production.production_package.*;
import mes.domain.entity.*;
import mes.domain.repository.*;
import mes.domain.services.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.production.service.ProdOrderEditService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/production/prod_order_edit")
public class ProdOrderEditController {

    @Autowired
    private ProdOrderEditService prodOrderEditService;

    @Autowired
    MaterialRepository materialRepository;

    @Autowired
    RoutingProcRepository routingProcRepository;

    @Autowired
    JobResRepository jobResRepository;

    @Autowired
    SujuRepository sujuRepository;

    @Autowired
    WorkcenterRepository workcenterRepository;

    @Autowired
    BomProcCompRepository bomProcCompRepository;

    @Autowired
    BomRepository bomRepository;

    @Autowired
    NotificationController notificationController;

    // 수주 목록 조회
    @GetMapping("/suju_list")
    public AjaxResult getSujuList(
            @RequestParam(value="date_kind", required=false) String date_kind,
            @RequestParam(value="start", required=false) String start,
            @RequestParam(value="end", required=false) String end,
            @RequestParam(value="mat_group", required=false) Integer mat_group,
            @RequestParam(value="factory", required=false) Integer cboFactory,
            @RequestParam(value="mat_name", required=false) String mat_name,
            @RequestParam("spjangcd") String spjangcd,
            @RequestParam(value="not_flag", required=false) String not_flag) {

        List<Map<String, Object>> items = this.prodOrderEditService.getSujuList(date_kind, start, end, mat_group, mat_name, not_flag, spjangcd, cboFactory);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    // 제품 지시내역 조회
    @GetMapping("/joborder_list")
    public AjaxResult getJobOrderList(
            @RequestParam(value="suju_id", required=false) Integer suju_id) {

        List<Map<String, Object>> items = this.prodOrderEditService.getJobOrderList(suju_id);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    // 제품 지시내역 상세조회
    @GetMapping("/joborder_detail")
    public AjaxResult getJobOrderDetail(
            @RequestParam("jobres_id") Integer jobres_id,
            HttpServletRequest request) {

        Map<String, Object> item = this.prodOrderEditService.getJobOrderDetail(jobres_id);

        AjaxResult result = new AjaxResult();
        result.data = item;

        return result;
    }

    // 반제품 작업지시 조회
    @GetMapping("/semi_list")
    public AjaxResult getSemiList(
            @RequestParam(value="data_date", required=false) String data_date,
            @RequestParam(value="mat_pk", required=false) Integer mat_pk,
            @RequestParam(value="suju_qty", required=false) Double suju_qty,
            @RequestParam(value="suju_pk", required=false) Integer suju_pk) {

        List<Map<String, Object>> items = this.prodOrderEditService.getSemiList(data_date, mat_pk, suju_qty, suju_pk);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    // 반제품 지시내역 조회
    @GetMapping("/semi_joborder_list")
    public AjaxResult getSemiJoborderList(
            @RequestParam(value="suju_id", required=false) Integer suju_id) {

        List<Map<String, Object>> items = this.prodOrderEditService.getSemiJoborderList(suju_id);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    // 작업지시 생성
    @PostMapping("/make_prod_order")
    @Transactional
    public AjaxResult makeProdOrder(
            @RequestParam(value="suju_id", required=false) Integer sujuId,
            @RequestParam(value="prod_date", required=false) String productionDate,
            @RequestParam(value="Material_id", required=false) Integer cboMaterial,
            @RequestParam(value="workshift", required=false) String cboShiftCode,
            @RequestParam(value="workcenter_id", required=false) Integer cboWorcenter,
            @RequestParam(value="equ_id", required=false) Integer cboEquipment,
            @RequestParam(value="AdditionalQty", required=false) Float txtOrderQty,
            @RequestParam("spjangcd") String spjangcd,
            HttpServletRequest request,
            Authentication auth) throws JsonProcessingException {

        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();

        //List<Map<String, Object>> bomListByMat = prodOrderEditService.getBomListByMat("11801");

        /*Map<Integer, BomNode> nodeMap = new HashMap<>();

        for(Map<String, Object> row : bomListByMat){
            BomNode node = new BomNode(row);
            nodeMap.put(node.myKey, node);
        }

        Map<String, BomNode> rootMap = new HashMap<>();

        for(BomNode node : nodeMap.values()){
            if(node.parentKey == null) {
                boolean hasFirstProcess = !node.class1.isEmpty();
                String key = hasFirstProcess ? "FIRST" : "SINGLE";

                rootMap.put(key, node);
            }
            else{
                BomNode parent = nodeMap.get(node.parentKey);
                if (parent != null) parent.children.add(node);
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        String json = mapper.writeValueAsString(rootMap);
        System.out.println(json);

        if(1==1){
            Toolkit defaultToolkit = Toolkit.getDefaultToolkit();
            defaultToolkit.beep();
            throw new CustomException("hh");
        }*/

        try{
            //작업지시 생성 (제품)
            JobRes header = prodOrderEditService.makeParentProdOrder(
                    sujuId,
                    productionDate,
                    cboMaterial,
                    cboShiftCode,
                    cboWorcenter,
                    cboEquipment,
                    txtOrderQty,
                    spjangcd,
                    user
            );

            result.success = true;
            result.data = header;
            return result;

        }catch(Exception e){
            throw new CustomException("예상치 못한 에러가 발생하였습니다.", e);
        }

        /*Integer matPk = cboMaterial;
        Material m = materialRepository.getMaterialById(matPk);
        Integer locPk = m.getStoreHouseId();
        Timestamp prodDate = CommonUtil.tryTimestamp(productionDate);

        // 신규 or 수정 검증
        JobRes header = new JobRes();

        // ===== 헤더 저장 =====
        header.set_audit(user);
        header.setProductionDate(prodDate);
        header.setProductionPlanDate(prodDate);
        header.setMaterialId(matPk);
        header.setOrderQty((float) txtOrderQty);
        header.setStoreHouse_id(locPk);
        header.setLotCount(1);
        header.setState("ordered");
        header.setSourceDataPk(sujuId);
        header.setSourceTableName("suju");
        header.setSpjangcd(spjangcd);

        //해당 수주 제품을 만들기 위한 공정들 구하기
        List<String> processCodes =  Stream.of(
                m.getClass1(), m.getClass2(), m.getClass3()
        ).filter(v -> v != null && !v.trim().isEmpty()).toList();

        if(processCodes.isEmpty()) throw new CustomException("해당 제품에 대한 등록된 공정이 없습니다.");

        header.setRouting_id(null);
        header.setProcessCount(processCodes.size());
        header.setWorkCenter_id(cboWorcenter);
        header.setFirstWorkCenter_id(cboWorcenter);
        header.setEquipment_id(cboEquipment);
        header.setShiftCode(cboShiftCode);
        header = jobResRepository.save(header); // 트리거가 번호 생성*/

        /*result.success = true;
        result.data = header;
        return result;
        */
    }

    // 작업 지시 수정 (모달 저장)
    //TODO: 진행중인 작업과의 커플링 고려
    @PostMapping("/update_order")
    @Transactional
    public AjaxResult updateOrder(
            @RequestParam(value="id", required=false) Integer jobres_id,
            @RequestParam(value="ProductionDate", required=false) String productionDate,
            @RequestParam(value="ShiftCode", required=false) String ShiftCode,
            @RequestParam(value="WorkCenter_id", required=false) Integer WorkCenter_id,
            @RequestParam(value="Equipment_id", required=false) Integer Equipment_id,
            @RequestParam(value="OrderQty", required=false) Float OrderQty,
            @RequestParam(value="Description", required=false) String Description,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();

        User user = (User)auth.getPrincipal();

        Timestamp ProductionDate = Timestamp.valueOf(productionDate + " 00:00:00");

        JobRes jr = this.jobResRepository.getJobResById(jobres_id);

        if (jr != null) {

            jr.setProductionDate(ProductionDate);
            jr.setShiftCode(ShiftCode);
            jr.setWorkCenter_id(WorkCenter_id);
            jr.setOrderQty(OrderQty);
            jr.setDescription(Description);
            if (Equipment_id != null) {
                jr.setEquipment_id(Equipment_id);
            }
            jr.set_audit(user);

            jr = this.jobResRepository.save(jr);

            result.success = true;
        } else {
            result.success = false;
        }

        return result;
    }

}