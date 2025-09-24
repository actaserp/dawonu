package mes.app.pda.controller;

import lombok.extern.slf4j.Slf4j;
import mes.app.pda.service.ShipmentApiService;
import mes.app.shipment.service.ShipmentDoBService;
import mes.app.shipment.service.ShipmentOrderService;
import mes.app.util.UtilClass;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/pda/shipment/shipment_order")
public class ShipmentApiController {

    @Autowired
    ShipmentApiService shipmentApiService;

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
}
