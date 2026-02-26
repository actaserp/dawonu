package mes.app.production;

import com.fasterxml.jackson.core.JsonProcessingException;
import mes.Exception.CustomException;
import mes.app.production.service.ProdOrderEditService;
import mes.domain.entity.JobRes;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.JobResRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/production/prod_order_edit")
public class ProdOrderEditController {

    @Autowired
    private ProdOrderEditService prodOrderEditService;

    @Autowired
    JobResRepository jobResRepository;
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

        List<JobRes> jobResList = jobResRepository.findBySourceDataPkAndSourceTableName(sujuId, "suju");

        if(jobResList.isEmpty()){
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

            return AjaxResult.success(null, header);
        }else{
            //작업지시 생성(반제품)
            //JobRes jobRes = prodOrderEditService.makeJobRes(cboMaterial);
            //return AjaxResult.success(null, jobRes);
            throw new CustomException("잘못된 작업지시 요청입니다.");
        }
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

    @PostMapping("/save_mat_match")
    @Transactional
    public AjaxResult saveMatMatch(
      @RequestParam("suju_item_id") Integer sujuItemId,
      @RequestParam("material_id") Integer materialId,
      @RequestParam("spjangcd") String spjangcd,
      Authentication auth
    ){
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();

        prodOrderEditService.saveMatMatch(sujuItemId, materialId, spjangcd, user);

        // 프론트 그리드 갱신용(선택)
//        Map<String,Object> mat = prodOrderEditService.getMatBasic(materialId, spjangcd);
        result.success = true;
//        result.data = mat; // {mat_code, mat_name, mat_type_name}
        return result;
    }


    @GetMapping("/search_mat_match")
    public AjaxResult getSearchMatMatch(
      @RequestParam(value="keyword", required=false) String keyword,
      @RequestParam(value="spjangcd", required=false) String spjangcd
    ){

        if (keyword == null || keyword.trim().isEmpty())    return AjaxResult.success(null, Collections.emptyList());

        List<Map<String,Object>> rows = prodOrderEditService.searchMatMatch(keyword, spjangcd);

        return AjaxResult.success(null, rows);
    }

}