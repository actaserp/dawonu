package mes.app.pda.controller;

import lombok.extern.slf4j.Slf4j;
import mes.app.pda.service.ShipmentApiService;
import mes.app.util.UtilClass;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MatLotRepository;
import mes.domain.repository.MatProcInputRepository;
import mes.domain.repository.MatProcInputReqRepository;
import mes.domain.services.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/pda/shipment/shipment_order")
public class ShipmentApiController {

    @Autowired
    ShipmentApiService shipmentApiService;

    @Autowired
    MatLotRepository matLotRepository;

    @Autowired
    MatProcInputRepository matProcInputRepository;

    @Autowired
    MatProcInputReqRepository matProcInputReqRepository;

    @PostMapping("/read")
    public AjaxResult getShipmentOrderList(
            @RequestParam(value="srchStartDt", required=false) String date_from,
            @RequestParam(value="srchEndDt", required=false) String date_to,
            @RequestParam(value="chkNotShipped", required=false) String not_ship,
            @RequestParam(value="cboCompany", required=false) Integer comp_pk,
            @RequestParam(value="cboMatGroup", required=false) Integer mat_grp_pk,
            @RequestParam(value="cboMaterial", required=false) Integer mat_pk,
            @RequestParam(value="keyword", required=false) String keyword,
            HttpServletRequest request) {

        String state = "";
        AjaxResult result = new AjaxResult();

        try{
            if("Y".equals(not_ship)) {
                state= "ordered";
            } else {
                state = "";
            }

            if(!date_from.contains("-")){
                date_from = UtilClass.toContainsHyphenDateString(date_from);
            }
            if(!date_to.contains("-")){
                date_to = UtilClass.toContainsHyphenDateString(date_to);
            }

            List<Map<String, Object>> items = this.shipmentApiService.ApigetShipmentOrderList(date_from, date_to, state, comp_pk, mat_grp_pk, mat_pk, keyword);
            //List<Map<String, Object>> test = Collections.emptyList();


            result.data = items;

        }catch(Exception e){
            result.success = false;
            result.data = null;
            result.message = "서버에러 발생";
        }

        return result;
    }

    @GetMapping("/shipment_list")
    public AjaxResult getShipmentList(
            @RequestParam(value = "header_id", required = false) Integer shipment_header_id,
            HttpServletRequest request) {

        List<Map<String, Object>> items = this.shipmentApiService.getShipmentList(shipment_header_id);

        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    //lot 바코드 스캔
    @GetMapping("/lot_scan")
    public AjaxResult getLotInfo(@RequestParam String lotNum, Authentication auth){

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        Timestamp inoutTime = DateUtil.getNowTimeStamp();

        //lot번호로 lot 정보 로드
        List<Map<String, Object>> results  = shipmentApiService.getByLotNumber(lotNum);

//        if(!results.isEmpty()){
//
//            MaterialLot ml = this.matLotRepository.getMatLotById((Integer) results.get(0).get("id"));
//            if(ml != null){
//                if(ml.getCurrentStock() <= 0){
//                    result.message = "가용한 재고가 없는 LOT을 지정했습니다.(" + ml.getLotNumber() + ")";
//                    result.success = false;
//                    return result;
//                }
//                if (ml.getStoreHouseId() == null) {
//                    result.message = "해당 품목의 기본창고가 지정되지 않았습니다(" + ml.getLotNumber() + ")";
//                    result.success = false;
//                    return result;
//                }
//
//                //TODO: 이미 지정된 로트 처리
////                List<MatProcInput> mpiList = this.matProcInputRepository.findByMaterialProcessInputRequestIdAndMaterialLotId(jr.getMaterialProcessInputRequestId(), ml.getId());
////                Integer mpiCount = mpiList.size();
////                if (mpiCount > 0) {
////                    result.message = "이미 지정된 로트입니다.(" + ml.getLotNumber() + ")";
////                    result.success = false;
////                    return result;
////                }
//                MatProcInputReq mir = null;
//                mir = new MatProcInputReq();
//                mir.setRequestDate(inoutTime);
//                mir.setRequesterId(user.getId());
//                mir.set_audit(user);
//                mir = this.matProcInputReqRepository.save(mir);
//
//            }else {
//                result.success = false;
//                result.message = lotNum + "에 대한 LOT 정보가 없습니다.";
//                result.data = null;
//            }
//
//            Map<String, Object> ml = results.get(0);
//            if(ml != null){
//                //if(ml.get(""))
//            }
//
//        }else{
//            result.success = false;
//            result.message = lotNum + "에 대한 LOT 정보가 없습니다.";
//            result.data = null;
//        }


        result.data = results.get(0);
        return result;
    }


    // lot 스캔
    @PostMapping("/add_lot_input")
    @Transactional
    public AjaxResult addLotInput(@RequestParam String lot_num,
                                  @RequestParam Float inputQty,
                                  @RequestParam Integer shipment_head_Id,
                                  Authentication authentication
                                  ){

        AjaxResult result = new AjaxResult();
        User user = (User) authentication.getPrincipal();

        Timestamp inoutTime = DateUtil.getNowTimeStamp();

        MaterialLot ml = this.matLotRepository.getByLotNumber(lot_num);

        if(ml != null){
            if(ml.getCurrentStock() <= 0){
                result.message = "가용한 재고가 없는 LOT을 지정했습니다.(" + ml.getLotNumber() + ")";
                result.success = false;
                return result;
            }

            if(ml.getStoreHouseId() == null){
                result.message = "해당 품목의 기본창고가 지정되지 않았습니다(" + ml.getLotNumber() + ")";
                result.success = false;
                return result;
            }

            ///mat_proc_input
            MatProcInputReq mir = null;

            mir = new MatProcInputReq();
            mir.setRequestDate(inoutTime);
            mir.setRequesterId(user.getId());
            mir.set_audit(user);
            mir = this.matProcInputReqRepository.save(mir);

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

//            ///mat_lot_cons 출고
//            Timestamp now = DateUtil.getNowTimeStamp();
//
//            MatLotCons mlc = new MatLotCons();
//            mlc.setOutputDateTime(now);
//            mlc.setSourceDataPk(shipment_head_Id);
//            mlc.setSourceTableName("shipmet_head");
//            mlc.setMaterialLotId(ml.getId());
//            mlc.setCurrentStock(ml.getCurrentStock());
//            mlc.setSpjangcd("ZZ");


            result.success = true;
            result.data = mpi;
        }else{
            result.success = false;
        }
        return result;
    }
}
