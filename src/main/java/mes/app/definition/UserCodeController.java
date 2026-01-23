package mes.app.definition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import mes.domain.entity.Material;
import mes.domain.entity.SystemCode;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.SysCodeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.definition.service.UserCodeService;
import mes.domain.entity.User;
import mes.domain.entity.UserCode;
import mes.domain.model.AjaxResult;
import mes.domain.repository.UserCodeRepository;


@RestController
@RequestMapping("/api/definition/code")
public class UserCodeController {
	
	
	@Autowired
	private UserCodeService codeService;

	@Autowired
	UserCodeRepository userCodeRepository;

	@Autowired
	MaterialRepository materialRepository;

	@Autowired
	SysCodeRepository sysCodeRepository;


	@GetMapping("/readFirst")
	public AjaxResult getCodeListFirst(
			@RequestParam("txtCode") String txtCode
			) {
		
		List<Map<String, Object>> items = this.codeService.getCodeListFirst(txtCode);
		AjaxResult result = new AjaxResult();
		
		result.data = items;
		return result;
	}

	@GetMapping("/readSecond")
	public AjaxResult getCodeListSecond(
			@RequestParam("txtCode") String txtCode,
			@RequestParam(value = "parentCode", required = false) String parentCode
	) {

		List<Map<String, Object>> items = this.codeService.getCodeListSecond(txtCode, parentCode);
		AjaxResult result = new AjaxResult();

		result.data = items;
		return result;
	}

	@GetMapping("/readThird")
	public AjaxResult getCodeListThird(
			@RequestParam("txtCode") String txtCode
	) {

		List<Map<String, Object>> items = this.codeService.getCodeListThird(txtCode);
		AjaxResult result = new AjaxResult();

		result.data = items;
		return result;
	}

	@GetMapping("/SystemCoderead")
	public AjaxResult getSystemCodeList(
			@RequestParam("txtCode") String txtCode,
			@RequestParam("txtCodeType") String txtCodeType,
			@RequestParam("Description") String Description,
			@RequestParam(value ="spjangcd") String spjangcd
	) {

		List<Map<String, Object>> items = this.codeService.getSystemCodeList(txtCode,txtCodeType,Description,spjangcd);
		AjaxResult result = new AjaxResult();

		result.data = items;
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getCode(@RequestParam("id") int id) {
		Map<String, Object> item = this.codeService.getCode(id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}

	@GetMapping("/Systemcodedetail")
	public AjaxResult getSystemCode(@RequestParam("id") int id,
									@RequestParam(value ="spjangcd") String spjangcd) {
		Map<String, Object> item = this.codeService.getSystemcCode(id,spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}

	@Transactional
	@PostMapping("/save")
	public AjaxResult saveCode(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("name") String value,
			@RequestParam("code") String code,
			@RequestParam(value="parent_id" , required=false) Integer parent_id,
			@RequestParam(value="parent_code" , required=false) String parent_code,
			@RequestParam(value="type" , required=false) String type,
			@RequestParam(value = "description", required = false) String description,
			@RequestParam("status") String status,
			HttpServletRequest request,
			Authentication auth) {
		User user = (User)auth.getPrincipal();
		
		UserCode c = null;
		
		if(id == null) {
			c = new UserCode();
		} else {
			c = this.userCodeRepository.getUserCodeById(id);
		}
		if(parent_code != null && !parent_code.isEmpty()) {
			List<UserCode> resultList = this.userCodeRepository.findByCode(parent_code);
			if (!resultList.isEmpty()) {
				UserCode p = resultList.get(0); // ✅ 첫 번째 데이터 사용
				c.setParentId(p.getId());
			}
		}else{
			c.setParentId(parent_id);
		}
		if(type != null && !type.isEmpty()) {
			c.setType(type);
		}
		c.setValue(value);
		c.setCode(code);
		c.setDescription(description);
		c.set_audit(user);
		c.set_status(status);

		// ✅ material 테이블 검사 및 insert 로직 추가
		boolean materialExists = materialRepository.existsByCode(code);
		if (!materialExists) {
			Material material = new Material();

			// 기본 정보
			material.setCode(code);
			material.setName(value);
			material.setDescription(description);

			// 필수 기본값들
			material.setStandardTimeUnit("min");
			material.setValidDays(90);
			material.setPurchaseOrderStandard("mrp");
			material.setVatExemptionYN("N");
			material.setMinOrder(1f);
			material.setMaxOrder(10f);
			material.setEquipment(102);
			material.setFactory_id(1);

			// ✅ type → MaterialGroup_id
			if (type != null && !type.isEmpty()) {
				try {
					material.setMaterialGroupId(Integer.parseInt(type));
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Invalid type (MaterialGroup_id): " + type);
				}
			}

			material.setStoreHouseId(5);
			material.setUnitId(3);
			material.setMtyn("1");
			material.setUseyn("0");
			material.setSpjangcd("ZZ");
			material.setClass1(code.length() >= 4 ? code.substring(0, 4) : code);
			material.setClass2(code);
			material.setClass3("");

			// 생성 정보
//			material.setCreatedBy(user.getUsername());
//			material.setCreatedAt(LocalDateTime.now());
			material.set_audit(user);

			materialRepository.save(material);
		}

		c = this.userCodeRepository.save(c);
		
		AjaxResult result = new AjaxResult();
		result.data = c;
		
		return result;
	}

	@PostMapping("/Systemcodesave")
	public AjaxResult Systemcodesave(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("code_type") String code_type,
			@RequestParam("name") String value,
			@RequestParam("code") String code,
			@RequestParam("description") String description,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		User user = (User)auth.getPrincipal();

		SystemCode s = null;

		if(id == null) {
			s = new SystemCode();
		} else {
			s = this.sysCodeRepository.getSysCodeById(id);
		}

		s.setCodeType(code_type);
		s.setValue(value);
		s.setCode(code);
		s.setDescription(description);
		s.set_audit(user);
		s.setSpjangcd(spjangcd);

		s = this.sysCodeRepository.save(s);

		AjaxResult result = new AjaxResult();
		result.data = s;

		return result;
	}


	@PostMapping("/delete")
	public AjaxResult deleteCode(@RequestParam("id") Integer id) {
		this.userCodeRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		
		return result;
	}


	@PostMapping("/SystemCodedelete")
	public AjaxResult deleteSystemCode(@RequestParam("id") Integer id) {
		this.sysCodeRepository.deleteById(id);
		AjaxResult result = new AjaxResult();

		return result;
	}

	@GetMapping("/getvalue")
	public AjaxResult getValue(@RequestParam("code") String code) {
		Map<String, Object> item = this.codeService.getValue(code);

		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}

	@GetMapping("/checkDuplicate")
	public ResponseEntity<Map<String, Boolean>> checkDuplicate(@RequestParam String code) {
		boolean exists = codeService.existsByCode(code);
		Map<String, Boolean> response = new HashMap<>();
		response.put("exists", exists);
		return ResponseEntity.ok(response);
	}


}