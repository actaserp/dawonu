package mes.app.definition;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import mes.app.definition.service.BomService;
import mes.domain.entity.*;
import mes.domain.repository.MaterialGroupRepository;
import mes.domain.repository.UserCodeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.definition.service.material.BomByMatService;
import mes.app.definition.service.material.MaterialService;
import mes.app.definition.service.material.RoutingByMatService;
import mes.app.definition.service.material.TestByMatService;
import mes.app.definition.service.material.UnitPriceService;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.TestMastMatRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/definition/material")
public class MaterialController {

	@Autowired
	private MaterialService materialService;

	@Autowired
	private UnitPriceService unitPriceService;

	@Autowired
	private BomByMatService bomService;

	@Autowired
	BomService bomServiceOfMaterial;

	@Autowired
	private RoutingByMatService routingService;

	@Autowired
	private TestByMatService testService;

	@Autowired
	MaterialRepository materialRepository;

	@Autowired
	TestMastMatRepository testMastMatRepository;

	@Autowired
	UserCodeRepository userCodeRepository;

	@Autowired
	MaterialGroupRepository materialGroupRepository;

	/**
	 * @apiNote 품목조회
	 *
	 * @param matType 품목구분
	 * @param matGroupId 품목그룹pk
	 * @param keyword 키워드
	 * @return
	 */
	@GetMapping("/read")
	public AjaxResult getMaterialList(
			@RequestParam(value = "mat_type", required = false) String matType,
			@RequestParam(value = "mat_group", required = false) String matGroupId,
			@RequestParam(value = "keyword", required = false) String keyword,
			@RequestParam(value ="spjangcd") String spjangcd,
			@RequestParam(value ="useYn_flag") String useYnFlag,
			@RequestParam(value ="user_code", required = false) Integer userCodeId
	) {

		List<Map<String, Object>> items = this.materialService.getMaterialList(matType, matGroupId, keyword,spjangcd, useYnFlag, userCodeId);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	/**
	 * @apiNote 품목상세조회
	 *
	 * @param matPk 품목pk
	 * @return
	 */
	@GetMapping("/detail")
	public AjaxResult getMaterial(@RequestParam("id") int matPk,
								  @RequestParam(value ="spjangcd") String spjangcd) {
		Map<String, Object> item = this.materialService.getMaterial(matPk,spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	@PostMapping("/batchDelete")
	public AjaxResult batchDelete(@RequestBody Map<String, Object> param) {
		List<Integer> ids = (List<Integer>) param.get("ids");
		AjaxResult result = new AjaxResult();
		int count = materialService.deleteMaterials(ids); // 여러개 삭제하는 서비스 메서드 필요
		result.success = count == ids.size();
		result.data = count;
		result.message = result.success ? "삭제 성공" : "삭제 실패";
		return result;
	}

	/**
	 * @apiNote 품목저장(생성/수정)
	 *
	 * @param data 품목정보
	 * @return
	 */
	@Transactional
	@PostMapping("/save")
	public AjaxResult saveMaterial(@RequestBody MultiValueMap<String,Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
		Authentication auth = sc.getAuthentication();
		User user = (User)auth.getPrincipal();
		data.set("user_id", user.getId());
		String spjangcd = user.getSpjangcd();

		AjaxResult result = new AjaxResult();

		int saveMatId = this.materialService.saveMaterial(data);

		// INSERT일 경우: 기본 BOM 자동 생성
		if (!data.containsKey("id") || data.getFirst("id") == null || data.getFirst("id").toString().isEmpty()) {
			try {
				// ==========================
				// ✅ 1. BOM 자동 생성
				// ==========================
				Bom bom = new Bom();
				String matName = data.getFirst("Name") != null ? data.getFirst("Name").toString() : "New Material";

				bom.setName(matName);
				bom.setMaterialId(saveMatId);
				bom.setOutputAmount(1F);
				bom.setBomType("manufacturing");
				bom.setVersion("1.0");
				bom.setStartDate(Timestamp.valueOf(LocalDate.now().atStartOfDay()));
				bom.setEndDate(Timestamp.valueOf("2100-12-31 23:59:59"));
				bom.set_audit(user);
				bom.setSpjangcd(spjangcd);

				this.bomServiceOfMaterial.saveBom(bom);

				// ==========================
				// ✅ 2. BOM_COMPONENT 자동 생성
				// ==========================
				BomComponent bomComp = new BomComponent();
				bomComp.setBomId(bom.getId());       // 생성된 BOM ID
				bomComp.setMaterialId(10853);
				bomComp.setAmount(1);
				bomComp.set_order(1);
				bomComp.setDescription("품목자동저장");
				bomComp.set_audit(user);

				this.bomServiceOfMaterial.saveBomComponent(bomComp);

				// ==========================
				// ✅ 3. 반환 데이터 구성
				// ==========================
				result.data = Map.of(
						"materialId", saveMatId,
						"bomId", bom.getId(),
						"bomComponentId", bomComp.getId()
				);
				result.success = true;
				result.message = "품목 및 BOM 자동등록 완료";

			} catch (Exception e) {
				e.printStackTrace();
				result.success = false;
				result.message = "BOM/구성 자동 생성 중 오류가 발생했습니다: " + e.getMessage();
				return result;
			}
		} else {
			result.data = Map.of("materialId", saveMatId);
			result.success = true;
			result.message = "품목 수정 완료";
		}

		return result;
	}

	/**
	 * @apiNote 품목삭제
	 *
	 * @param matPk 품목pk
	 * @return
	 */
	@PostMapping("/delete")
	public AjaxResult deleteMaterial(@RequestParam("id") int matPk) {

		AjaxResult result = new AjaxResult();

		if (this.materialService.deleteMaterial(matPk) > 0) {

		} else {
			result.success = false;
		};

		return result;
	}

	/**
	 * @apiNote 품목 업체별 단가조회
	 *
	 * @param matPk 품목pk
	 * @return
	 */
	@GetMapping("/readPrice")
	public AjaxResult getPriceList(@RequestParam("mat_pk") int matPk) {

		List<Map<String, Object>> items = this.unitPriceService.getPriceListByMat(matPk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	/**
	 * @apiNote 품목 업체별 단가이력 조회
	 *
	 * @param matPk 품목pk
	 * @return
	 */
	@GetMapping("/readPriceHistory")
	public AjaxResult getPriceHistory(@RequestParam("mat_pk") int matPk,
									  @RequestParam("com_pk") int comPk) {

		List<Map<String, Object>> items = this.unitPriceService.getPriceHistoryByMat(matPk,comPk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	/**
	 * @apiNote 품목 업체별 단가상세조회
	 *
	 * @param pricePk 단가pk
	 * @return
	 */
	@GetMapping("/detailPrice")
	public AjaxResult getPriceDetail(@RequestParam("price_id") int pricePk) {

		Map<String, Object> item = this.unitPriceService.getPriceDetail(pricePk);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	/**
	 * @apiNote 단가저장(등록/변경)
	 *
	 * @param data 품목단가정보
	 * @return
	 */
	@PostMapping("/savePrice")
	public AjaxResult savePriceByMat(@RequestBody MultiValueMap<String,Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
		Authentication auth = sc.getAuthentication();
		User user = (User)auth.getPrincipal();
		data.set("user_id", user.getId());

		AjaxResult result = new AjaxResult();

		try {
			int saveCount = this.unitPriceService.saveCompanyUnitPrice(data);

			if (saveCount > 0) {
				result.success = true;
			} else {
				result.success = false;
				result.message = "저장 실패: 중복된 데이터이거나 입력값이 올바르지 않습니다.";
			}
		} catch (Exception e) {
			result.success = false;
			result.message = "서버 오류: " + e.getMessage();  // 예외 메시지 포함
		}

		return result;
	}

	/**
	 * @apiNote 단가수정
	 *
	 * @param data 품목단가정보
	 * @return
	 */
	@PostMapping("/updatePrice")
	public AjaxResult updatePriceByMat(@RequestBody MultiValueMap<String,Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
		Authentication auth = sc.getAuthentication();
		User user = (User)auth.getPrincipal();
		data.set("user_id", user.getId());

		AjaxResult result = new AjaxResult();

		if (this.unitPriceService.updateCompanyUnitPrice(data) > 0) {

		} else {
			result.success = false;
		};

		return result;
	}

	/**
	 * @apiNote 단가삭제
	 *
	 * @param priceId 단가pk
	 * @return
	 */
	@PostMapping("/deletePrice")
	public AjaxResult deletePriceByMat(@RequestParam("id") int priceId) {

		AjaxResult result = new AjaxResult();

		if (this.unitPriceService.deleteCompanyUnitPrice(priceId) > 0) {

		} else {
			result.success = false;
		};

		return result;
	}

	/**
	 * @apiNote BOM목록조회
	 *
	 * @param matPk
	 * @return
	 */
	@GetMapping("/bom")
	public AjaxResult readBomList(@RequestParam("mat_id") String matPk) {
		List<Map<String, Object>> items = this.bomService.getBomListByMat(matPk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	/**
	 * @apiNote BOM목록조회
	 *
	 * @param matPk
	 * @return
	 */
	@GetMapping("/bomReverse")
	public AjaxResult readBomReverseList(@RequestParam("mat_id") int matPk) {
		List<Map<String, Object>> items = this.bomService.getBomReverseListByMat(matPk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	/**
	 * @apiNote 라우팅목록조회
	 *
	 * @param routingPk
	 * @return
	 */
	@GetMapping("/routingProcess")
	public AjaxResult readRoutingProcessList(@RequestParam("routing_pk") String routingPk) {
		List<Map<String, Object>> items = this.routingService.getRoutingProcessList(routingPk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	/**
	 * @apiNote 검사정보조회
	 *
	 * @param matPk
	 * @return
	 */
	@GetMapping("/testMaster")
	public AjaxResult readTestMasterList(@RequestParam("mat_id") int matPk) {
		List<Map<String, Object>> items = this.testService.getTestMasterList(matPk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	@PostMapping("/save_test_master")
	@Transactional
	public AjaxResult saveTestMaster(
			@RequestParam(value = "mat_id", required = false) Integer matId,
			@RequestParam(value = "InTestYN", required = false) String inTestYN,
			@RequestParam(value = "OutTestYN", required = false) String outTestYN,
			@RequestParam MultiValueMap<String, Object> Q,
			@RequestParam(value = "deletedId", required = false) Integer deletedId, // 삭제할 ID 추가
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();
		List<TestMastMat> savedData = new ArrayList<>();

		// Material 업데이트
		if (matId != null && matId > 0) {
			Material m = materialRepository.getMaterialById(matId);
			m.setInTestYN(inTestYN);
			m.setOutTestYN(outTestYN);
			m.set_audit(user);
			materialRepository.save(m);
		}

		// 삭제 처리
		if (deletedId != null && deletedId > 0) {
			testMastMatRepository.deleteById(deletedId);
		}

		// 남은 데이터 업데이트
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		for (Map<String, Object> item : data) {
			Integer id = item.get("id") != null ? Integer.parseInt(item.get("id").toString()) : null;
			Integer testMasterId = Integer.parseInt(item.get("test_master_id").toString());

			Optional<TestMastMat> existing = testMastMatRepository.findByMaterialIdAndTestMasterId(matId, testMasterId);

			if (existing.isPresent()) {
				TestMastMat tm = existing.get();
				tm.set_audit(user);
				savedData.add(testMastMatRepository.save(tm)); // 수정된 데이터 추가
			} else {
				TestMastMat tm = new TestMastMat();
				tm.setMaterialId(matId);
				tm.setTestMasterId(testMasterId);
				tm.set_audit(user);
				savedData.add(testMastMatRepository.save(tm)); // 새 데이터 추가
			}
		}

		// 최신 데이터 조회 및 반환
		List<TestMastMat> finalData = testMastMatRepository.findByMaterialId(matId);
		result.data = finalData;

		return result;
	}
	// 라우팅 정보 저장(material update)
	@PostMapping("/saveRouting")
	public AjaxResult saveRouting(@RequestParam("mat_id") Integer matId,
								  @RequestParam(value = "routing_pk", required = false) Integer routingPk) {
		int updateRow = this.materialService.saveRouting(matId, routingPk);

		AjaxResult result = new AjaxResult();
		result.data = updateRow;

		return result;
	}

	@GetMapping("/children")
	public AjaxResult getChildren(@RequestParam("parentId") Integer parentId) {
		AjaxResult result = new AjaxResult();
		try {
			// 1️⃣ parentId는 material_group.id 로 들어옴
			Optional<MaterialGroup> matGrpOpt = materialGroupRepository.findById(parentId);
			if (matGrpOpt.isEmpty()) {
				result.success = false;
				result.message = "해당 품목그룹을 찾을 수 없습니다.";
				return result;
			}
			MaterialGroup matGrp = matGrpOpt.get();

			// 2️⃣ material_group.name = user_code.value 매핑
			UserCode parentCode = userCodeRepository.findByCode(matGrp.getCode())
					.stream().findFirst()
					.orElse(null);

			if (parentCode == null) {
				result.success = false;
				result.message = "대응되는 UserCode(대분류)가 없습니다.";
				return result;
			}

			// 3️⃣ user_code.children 조회
			List<UserCode> children = userCodeRepository.findByParentId(parentCode.getId());

			List<Map<String, Object>> items = children.stream()
					.map(c -> {
						Map<String, Object> map = new HashMap<>();
						map.put("id", c.getId());
						map.put("code", c.getCode());
						map.put("value", c.getValue());
						return map;
					})
					.collect(Collectors.toList());

			result.success = true;
			result.data = items;

		} catch (Exception e) {
			result.success = false;
			result.message = "조회 실패: " + e.getMessage();
		}
		return result;
	}

}
