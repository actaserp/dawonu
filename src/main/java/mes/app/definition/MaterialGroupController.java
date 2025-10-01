package mes.app.definition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import mes.domain.entity.UserCode;
import mes.domain.repository.UserCodeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.domain.repository.MaterialGroupRepository;
import mes.app.definition.service.material.MaterialGroupService;
import mes.domain.entity.MaterialGroup;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/definition/material_group")
public class MaterialGroupController {
	
	@Autowired
	MaterialGroupRepository materialGroupRepository;
	
	@Autowired
	private MaterialGroupService materialGroupService;

	@Autowired
	UserCodeRepository userCodeRepository;
	
	// 품목그룹 목록 조회
	@GetMapping("/read")
	public AjaxResult getMatGrouptList(
			@RequestParam("mat_type") String matType,
			@RequestParam("mat_grp") String matGrp,
			@RequestParam(value ="spjangcd") String spjangcd,
    		HttpServletRequest request) {
       
        List<Map<String, Object>> items = this.materialGroupService.getMatGrouptList(matType, matGrp,spjangcd);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	// 품목그룹 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getMatGroupDetail(
			@RequestParam("id") int matGrpId, 
    		HttpServletRequest request) {
        Map<String, Object> item = this.materialGroupService.getMatGroupDetail(matGrpId);      
               		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	// 품목그롭 저장
	@PostMapping("/save")
	public AjaxResult saveMatGroup(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("material_type") String matType,
			@RequestParam("material_group_code") String code,
			@RequestParam("material_group_name") String name,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		MaterialGroup matGrp = null;
		AjaxResult result = new AjaxResult();
		
		boolean code_chk = this.materialGroupRepository.findByCode(code).isEmpty();
		
		boolean name_chk = this.materialGroupRepository.findByName(name).isEmpty();
		
		if(id == null) {
			matGrp = new MaterialGroup();
		} else {
			matGrp = this.materialGroupRepository.getMatGrpById(id);
		}
		
		if (code.equals(matGrp.getCode())==false && code_chk == false) {
			result.success = false;
			result.message="중복된 설비그룹코드가 존재합니다.";
			return result;
		}
		
		if (name.equals(matGrp.getName())==false && name_chk == false) {
			result.success = false;
			result.message="중복된 설비그룹명이 존재합니다.";
			return result;
		}

		// ✅ MaterialGroup 저장
		matGrp.setName(name);
		matGrp.setCode(code);
		matGrp.setMaterialType(matType);
		matGrp.set_audit(user);
		matGrp.setSpjangcd(spjangcd);

		matGrp = this.materialGroupRepository.save(matGrp);

		// ✅ UserCode 동기화 저장
		List<UserCode> list = this.userCodeRepository.findByCode(code);
		UserCode uc = list.isEmpty() ? new UserCode() : list.get(0);

		uc.setCode(code);
		uc.setValue(name);
		uc.setDescription("품목그룹 자동 생성");
		uc.set_audit(user);

		this.userCodeRepository.save(uc);
		
        result.data=matGrp;
		return result;
	}

	// 품목그룹 삭제
	@PostMapping("/delete")
	public AjaxResult deleteMatGroup(@RequestParam("id") Integer id) {
		AjaxResult result = new AjaxResult();

		try {
			// 1️⃣ 우선 material_group 조회
			Optional<MaterialGroup> optional = materialGroupRepository.findById(id);
			if (optional.isEmpty()) {
				result.success = false;
				result.message = "삭제할 품목그룹을 찾을 수 없습니다.";
				return result;
			}

			MaterialGroup matGrp = optional.get();

			// 2️⃣ user_code에서 해당 그룹에 대응하는 값 찾아 삭제
			//    (Value = material_group_name 이었던 항목)
			List<UserCode> codes = userCodeRepository.findByCode(matGrp.getCode());
			if (!codes.isEmpty()) {
				userCodeRepository.deleteAll(codes);
			}

			// 3️⃣ material_group 삭제
			materialGroupRepository.deleteById(id);

			result.success = true;
			result.message = "삭제되었습니다.";
		} catch (Exception e) {
			result.success = false;
			result.message = "삭제 실패: " + e.getMessage();
		}

		return result;
	}


	// 중분류 리스트 조회
	@GetMapping("/userCodeList")
	public AjaxResult getUserCodes(@RequestParam("parent_id") Integer parentId) {
		AjaxResult result = new AjaxResult();
		try {
			List<UserCode> codes = userCodeRepository.findByParentId(parentId);
			result.success = true;
			result.data = codes;
		} catch (Exception e) {
			result.success = false;
			result.message = "조회 중 오류 발생: " + e.getMessage();
		}
		return result;
	}

	// 대분류 리스트 조회
	@GetMapping("/parent")
	public AjaxResult getChildren(@RequestParam(value = "parentId", required = false) Integer parentId) {
		AjaxResult result = new AjaxResult();
		try {
			List<UserCode> children = userCodeRepository.findByParentId(parentId);

			// 필요한 데이터만 반환 (id, code, value 정도)
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

	// 중분류 저장 (insert/update)
	@PostMapping("/saveUserCode")
	public AjaxResult saveUserCode(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("parent_id") Integer parentId,
			@RequestParam("code") String code,
			@RequestParam("value") String value,
			@RequestParam(value="description", required=false) String description,
			@RequestParam(value="status", required=false, defaultValue="y") String status,
			Authentication auth) {

		User user = (User) auth.getPrincipal();
		UserCode uc = (id == null) ? new UserCode() : userCodeRepository.findById(id).orElse(new UserCode());

		uc.setParentId(parentId);
		uc.setCode(code);
		uc.setValue(value);
		uc.setDescription(description);
		uc.set_audit(user);

		userCodeRepository.save(uc);

		AjaxResult result = new AjaxResult();
		result.success = true;
		result.data = uc;
		return result;
	}

	// 중분류 삭제
	@PostMapping("/deleteUserCode")
	public AjaxResult deleteUserCode(@RequestParam("id") Integer id) {
		userCodeRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		result.success = true;
		return result;
	}


}
