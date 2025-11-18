package mes.app.shipment;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;
import mes.app.shipment.enums.ShipmentStatus;
import mes.domain.entity.*;
import mes.domain.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import mes.app.shipment.service.ShipmentDoBService;
import mes.domain.model.AjaxResult;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;

@RestController
@RequestMapping("/api/shipment/shipment_do_b")
@Slf4j
public class ShipmentDoBController {
	@Autowired
	private MatLotRepository matLotRepository;

	@Autowired
	private ShipmentDoBService shipmentDoBService;

	@Autowired
	MatLotConsRepository matLotConsRepository;

	@Autowired
	TransactionTemplate transactionTemplate;

	@Autowired
	ShipmentRepository shipmentRepository;

	@Autowired
	MaterialRepository materialRepository;
    @Autowired
    ShipmentHeadRepository shipmentHeadRepository;

	// 출하지시헤더 조회
	@GetMapping("/read")
	public AjaxResult getShipmentHeaderList(
			@RequestParam(value = "srchStartDt", required = false) String date_from,
			@RequestParam(value = "srchEndDt", required = false) String date_to,
			@RequestParam(value = "cboCompany", required = false) Integer comp_pk,
			@RequestParam(value = "cboMatGroup", required = false) Integer mat_grp_pk,
			@RequestParam(value = "cboMaterial", required = false) Integer mat_pk,
			@RequestParam(value = "keyword", required = false) String keyword,
			@RequestParam(value = "chkNotShipped", required = false) String not_ship, HttpServletRequest request) {

		String state = "";
		if ("Y".equals(not_ship)) {
			state = "ordered";
		} else {
			state = "";
		}

		List<Map<String, Object>> items = this.shipmentDoBService.getShipmentHeaderList(date_from, date_to, state,
				comp_pk, mat_grp_pk, mat_pk, keyword);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 출하 항목 조회
	@GetMapping("/shipment_list")
	public AjaxResult getShipmentList(
			@RequestParam(value = "header_id", required = false) Integer shipment_header_id,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.shipmentDoBService.getShipmentList(shipment_header_id);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 출하 처리 LOT상세
	@GetMapping("/shipment_lot_list")
	public AjaxResult getShipmentLotList(
			@RequestParam(value = "sh_id", required = false) Integer sh_id,
			@RequestParam(value = "shipment_id", required = false) Integer shipment_id, HttpServletRequest request) {

		List<Map<String, Object>> items = this.shipmentDoBService.getShipmentLotList(sh_id, shipment_id);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// LOT지정 팝업 lot 검색
	@GetMapping("/mat_lot_search")
	public AjaxResult getMatLotSearch(
			@RequestParam(value = "sh_id", required = false) Integer sh_id,
			@RequestParam(value = "material_id", required = false) Integer material_id,
			@RequestParam(value = "lot_number", required = false) String lot_number, HttpServletRequest request) {

		List<Map<String, Object>> items = this.shipmentDoBService.getMatLotSearch(sh_id, material_id, lot_number);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// lot 추가, TODO: LOT추가하면 mat_lot_cons에는 추가되고 mat_lot의 현재고도 깎이는데 material은 현재고가 변함이 없음 --> 뭐지? 출하할때는 빠지나 확인필요.
	@PostMapping("/save_mat_lot_cons")
	public AjaxResult saveMatLotCons(
			@RequestParam(value = "sh_id") Integer sh_id,
			@RequestParam(value = "shipment_id") Integer shipment_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		Optional<Shipment> shipment = shipmentRepository.findById(shipment_id);

		if(shipment.isPresent()){
//			Shipment shipment1 = shipment.get();
//			if(shipment1.getQty() >= shipment1.getOrderQty()){
//				result.success = false;
//				result.message = "이미 지시량만큼 로트가 지정되었습니다.";
//				return result;
//			}

		}else{
			result.success = false;
			result.message = "출하상세정보가 없습니다.";
			return result;
		}

		String sourceData = shipment.get().getSourceTableName();

		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		Timestamp now = DateUtil.getNowTimeStamp();

		if (items.size() == 0) {
			result.success = false;
			return result;
		}

		//validation check
		for (int i = 0; i < items.size(); i++) {
			Integer ml_id = (Integer) items.get(i).get("ml_id");
			Optional<MaterialLot> mat_lot = matLotRepository.findById(ml_id);

			if(mat_lot.isPresent()){
				MaterialLot materialLot = mat_lot.get();
				Float quantity = Float.valueOf((String)items.get(i).get("quantity"));
				Float currentStock = materialLot.getCurrentStock();

				if(quantity > currentStock){
					result.success = false;
					result.message = "현재고가 부족합니다.";
					return result;
				}

			}else{
				result.success = false;
				result.message = "해당 lot에 대한 정보가 없습니다.";
				return result;
			}
		}

		this.transactionTemplate.executeWithoutResult(status->{
			for (int i = 0; i < items.size(); i++) {
	            Integer ml_id = (Integer) items.get(i).get("ml_id");


	            Float quantity = Float.valueOf((String)items.get(i).get("quantity"));
				Double doubleQty = Double.valueOf(String.format("%.2f", quantity));

	            Integer mlc_id = (Integer) items.get(i).get("mlc_id");

				MatLotCons matlotcons = null;


				if (mlc_id != null) {
					matlotcons = this.matLotConsRepository.getMatLotConsById(mlc_id);
				} else {
					matlotcons = new MatLotCons();
					matlotcons.setMaterialLotId(ml_id);
					matlotcons.setSourceDataPk(shipment_id);
					matlotcons.setSourceTableName("shipment");
				}
				matlotcons.setOutputDateTime(now);
				matlotcons.setOutputQty(doubleQty.floatValue());
				matlotcons.set_audit(user);

				this.matLotConsRepository.save(matlotcons);
				//this.shipmentDoBService.updateShipmentQantityByLotConsume(sh_id, shipment_id, sourceData);
				this.shipmentDoBService.updateShipmentAndHeadByLotConsume(sh_id, shipment_id, sourceData);
			}
		});

		return result;
	}

	// Lot 삭제
	@PostMapping("/delete_mal_lot_consume")
	public AjaxResult deleteMalLotConsume(
			@RequestBody MultiValueMap<String,Object> Q,
			@RequestParam(value = "sh_id", required = false) Integer sh_id,
			@RequestParam(value = "shipment_id", required = false) Integer shipment_id,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();

		List<Integer> mlc_ids = loadJsonList(Q.getFirst("Q").toString());

		if (mlc_ids != null && mlc_ids.size() > 0) {
			this.transactionTemplate.executeWithoutResult(status -> {
				try {
					for (int i = 0; i < mlc_ids.size(); i++) {
						Integer mlc_id = mlc_ids.get(i);
						this.shipmentDoBService.deleteMatLotCons(mlc_id);
					}
					Optional<Shipment> shipment = this.shipmentRepository.findById(shipment_id);
					String sourceData = "";
					if(shipment.isPresent()){
						sourceData = shipment.get().getSourceTableName();
					}else{
						throw new RuntimeException("출하상세정보가 없습니다.");
					}

					//this.shipmentDoBService.updateShipmentQantityByLotConsume(sh_id, shipment_id, sourceData);
					this.shipmentDoBService.updateShipmentAndHeadByLotConsume(sh_id, shipment_id, sourceData);



				} catch (Exception e) {
					// 로그 출력
					e.printStackTrace();  // 콘솔 출력
					log.error("트랜잭션 처리 중 오류 발생: {}", e.getMessage(), e);
					// 롤백 표시
					status.setRollbackOnly();
					throw e; // 다시 던져줘야 rollback 확실하게 됨
				}
			});
		} else {
			result.success = false;
			return result;
		}

		return result;
	}

	// 출고 처리
	@PostMapping("/shipment_status_complete")
	public AjaxResult shipmentStatusComplete(
			@RequestParam(value = "sh_id", required = false) Integer sh_id,
			@RequestParam(value = "description", required = false) String description,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		// Validation 체크 - 출하처리시 Material.currentStock 값과 Shipment.Qty(처리량)을 비교하여 값이 '-'인 경우 출하가 되지않도록 처리
		List<Shipment> shipmentList = this.shipmentRepository.findByShipmentHeadId(sh_id);

		if (shipmentList != null){
			for (int i = 0; i < shipmentList.size(); i++) {
				Integer materialId = shipmentList.get(i).getMaterialId();
				Material material = this.materialRepository.getMaterialById(materialId);

				if (material != null) {
					Float currentStock = material.getCurrentStock() != null ? material.getCurrentStock() : 0;
					Double shipQty = shipmentList.get(i).getQty() != null ? shipmentList.get(i).getQty() : 0;
					Double orderQty = shipmentList.get(i).getOrderQty();

					Float parsedShipQty = shipQty.floatValue();

					if (Float.compare(currentStock, parsedShipQty) < 0) {
						result.success = false;
						result.message = "재고 수량이 부족합니다.";
						return result;
					}

					if(orderQty > shipQty){
						result.success = false;
						result.message = "LOT가 지시량만큼 지정되지 않은 상세가 있습니다.";
						return result;
					}
				}
			}
		}

		this.transactionTemplate.executeWithoutResult(status->{

			// Shipment 테이블의 상태값 변경시 트리거 사용하여 "a"로 설정시 트리거를 통해 mat_inout 테이블에 출고데이터가 추가됨
			List<Shipment> smList = this.shipmentRepository.findByShipmentHeadId(sh_id);

			if (smList != null) {

				double orderSum = 0;
				for (int i = 0; i < smList.size(); i++) {
					Shipment sm = smList.get(i);

					if (sm != null) {
						Double orderQty = sm.getOrderQty();
						sm.set_status("a");
						sm.set_audit(user);
						//sm.setQty(orderQty);

						this.shipmentRepository.save(sm);

						orderSum += orderQty;
					}
				}
			}
			String sourceData = smList.get(0).getSourceTableName() == null ? "" : smList.get(0).getSourceTableName();

			// 수주헤더 기준으로 출하항목(shipment) 금액합산 정리
			this.shipmentDoBService.updateShipmentStateComplete(sh_id, description, sourceData);
			// 관련 수주를 찾아서 수주의 출하 상태를 변경한다.
			this.shipmentDoBService.updateSujuShipmentState(sh_id);

		});

		return result;
	}

	private static List<Integer> loadJsonList(String strJson) {

		ObjectMapper objectMapper = new ObjectMapper();
		List<Integer> result = null;
		try {
			result = objectMapper.readValue(strJson, new TypeReference<List<Integer>>(){});
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		return result;
	}
}
