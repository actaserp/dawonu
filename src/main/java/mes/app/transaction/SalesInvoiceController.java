package mes.app.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.openhtmltopdf.outputdevice.helper.BaseRendererBuilder;
import mes.app.aop.DecryptField;
import mes.app.transaction.service.SalesInvoiceService;
import mes.config.Settings;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CompanyRepository;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.TB_SalesDetailRepository;
import mes.domain.repository.TB_SalesmentRepository;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api/tran/sales")
public class SalesInvoiceController {
	
	@Autowired
	private SalesInvoiceService salesInvoiceService;
    @Autowired
    private CompanyRepository companyRepository;
    @Autowired
    private MaterialRepository materialRepository;
	@Autowired
	Settings settings;

	@Autowired
	private TB_SalesmentRepository salesmentRepository;

	@Autowired
	private TB_SalesDetailRepository salesDetailRepository;

	@GetMapping("/shipment_head_list")
	public AjaxResult getShipmentHeadList(
			@RequestParam("srchStartDt") String dateFrom,
			@RequestParam("srchEndDt") String dateTo,
			@RequestParam(value="comp_id", required=false) Integer cltcd
	) {
		
		List<Map<String, Object>> items = this.salesInvoiceService.getShipmentHeadList(dateFrom,dateTo, cltcd);

		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@GetMapping("/get_material")
	public AjaxResult getMaterialName(
			@RequestParam("material_id") Integer id
	) {

		Material item = materialRepository.getMaterialById(id);

		Map<String, Object> data = new HashMap<>();
		data.put("material_name", item.getName());
		data.put("spec", item.getStandard1());

		AjaxResult result = new AjaxResult();
		result.data = data;
		return result;
	}

	@GetMapping("/get_suju")
	public AjaxResult getSuju(
			@RequestParam("suju_id") Integer id
	) {

		Map<String, Object> item = this.salesInvoiceService.getSuju(id);

		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}


	// 공급자(사업장) 정보 가져오기
	@GetMapping("/invoicer_read")
	public AjaxResult getInvoicerDatail(
			@RequestParam("spjangcd") String spjangcd
	) {

		Map<String, Object> item = this.salesInvoiceService.getInvoicerDatail(spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	// 공급받는자 휴폐업 조회
	@PostMapping("/invoicee_check")
	public AjaxResult invoiceeCheck(
			@RequestParam("b_no") String bno, // 사업자 번호
			@RequestParam("compid") Integer compid, // company id
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		try {
			JsonNode data = salesInvoiceService.validateSingleBusiness(bno);

			if (data == null) {
				result.success = false;
				result.message = "계속사업자가 아니므로 거래중지 처리하시겠습니까?";
				result.code = "STOP_CONFIRM"; // 추가
				return result;
			}

			String statusCode = data.path("b_stt_cd").asText();
			String statusText = data.path("b_stt").asText();
			String taxTypeText = data.path("tax_type").asText();

			if ("01".equals(statusCode)) {
				result.success = true;
				result.data = data; // 단건 결과 JSON 문자열로 반환
			} else {
				Company company = companyRepository.getCompanyById(compid);
				company.setRelyn("1");
				companyRepository.save(company);

				if (statusText == null || statusText.isBlank()) {
					result.success = false;
					result.message = taxTypeText + "\n거래중지 처리되었습니다.";
				} else {
					result.success = false;
					result.message = "사업자 상태: " + statusText + " 거래중지 처리되었습니다.";
				}
			}

		} catch (Exception e) {
			result.success = false;
			result.message = "사업자 진위 확인 실패: " + e.getMessage();
		}

		return result;
	}

	@PostMapping("/invoicee_stop")
	public AjaxResult stopInvoicee(@RequestParam("compid") Integer compid) {
		AjaxResult result = new AjaxResult();
		try {
			Company company = companyRepository.getCompanyById(compid);
			company.setRelyn("1");
			companyRepository.save(company);
			result.success = true;
			result.message = "거래중지 처리되었습니다.";
		} catch (Exception e) {
			result.success = false;
			result.message = "거래중지 처리 실패: " + e.getMessage();
		}
		return result;
	}

	// 공급받는자 저장
	@PostMapping("/invoicee_save")
	public AjaxResult invoiceeSave(
			@RequestParam Map<String, Object> paramMap,
			Authentication auth) {

		User user = (User) auth.getPrincipal();
		return salesInvoiceService.saveInvoicee(paramMap, user);
	}

	// 검색
	@DecryptField(columns = {"ivercorpnum"}, masks = 6)
	@GetMapping("/read")
	public AjaxResult getInvoiceList(
			@RequestParam(value="invoice_kind", required=false) String invoice_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="cboCompany", required=false) Integer cboCompany,
			@RequestParam(value="cboStatecode", required=false) Integer cboStatecode,
			@RequestParam(value="spjangcd", required=false) String spjangcd,
			HttpServletRequest request) {

		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.salesInvoiceService.getList(invoice_kind, cboStatecode, cboCompany, start, end, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	//초가데이터 삽입용
	@GetMapping("/read_excel")
	@Transactional
	public AjaxResult setInitData2() {
		String filePath = "C:\\Users\\User\\Desktop\\신우\\invoicement_data.xls";
		AjaxResult result = new AjaxResult();

		try (FileInputStream fis = new FileInputStream(filePath);
			 Workbook workbook = new HSSFWorkbook(fis)) {

			// ✅ 수식 평가기 생성
			FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
			Sheet sheet = workbook.getSheet("매출");
			if (sheet == null) {
				result.message = "시트 '매출'을 찾을 수 없습니다.";
				return result;
			}

			int misseq = 1;

			for (int i = 1; i <= sheet.getLastRowNum(); i++) {
				Row row = sheet.getRow(i);
				if (row == null) continue;

				TB_Salesment tb = new TB_Salesment();
				String misdate = "";
				// ✅ 날짜 셀 처리
				Cell cellC = row.getCell(2);
				if (cellC != null) {
					 misdate = getDateString(cellC);
					tb.setMisdate(misdate);
				}

				// ✅ 거래처 코드 (VLOOKUP 계산 포함)
				Integer cltcd = getIntegerValue(row.getCell(4), evaluator);
				if (cltcd == null) {
					System.out.println("❌ 거래처 코드 누락됨: " + (i + 1) + "행");
					throw new RuntimeException("거래처코드없음");
				}

				Optional<Company> byId = companyRepository.findById(cltcd);
				if (byId.isEmpty()) {
					throw new RuntimeException("거래처없는데? 코드=" + cltcd);
				}

				tb.setCltcd(cltcd);
				tb.setIssuetype("정발행");
				tb.setTaxtype("과세");
				tb.setPurposetype("청구");
				tb.setSupplycost(getIntMoney(row.getCell(9), evaluator));
				tb.setTaxtotal(getIntMoney(row.getCell(10), evaluator));
				tb.setTotalamt(getIntMoney(row.getCell(11), evaluator));
				tb.setRemark1(truncate(getStringValue(row.getCell(15), evaluator), 1000));

				tb.setIcercorpnum("3178143546");
				tb.setIcercorpnm("신우테크산업 주식회사");
				tb.setIcerceonm("임덕빈");
				tb.setIceraddr("충북 청주시 서원구 남이면 남석로 468-24");
				tb.setIcerbiztype("제조업,도매및소매업");

				tb.setIvercorpnum(byId.get().getBusinessNumber());
				tb.setIvercorpnm(byId.get().getName());
				tb.setIveraddr(byId.get().getAddress());
				tb.setMisgubun("etc_sale");
				tb.setSpjangcd("ZZ");
				tb.setIssuediv("other"); //타사이트발행
				tb.setInvoiceetype("사업자");

				// ✅ 저장
				TB_Salesment saved = salesmentRepository.save(tb);

				// 자식 디테일 생성
				TB_SalesDetail detail = new TB_SalesDetail();
				detail.setMisdate(misdate);
				detail.setItemnm(getStringValue(row.getCell(8), evaluator));
				detail.setSupplycost(getIntMoney(row.getCell(9), evaluator));
				detail.setTaxtotal(getIntMoney(row.getCell(10), evaluator));
				detail.setTotalamt(getIntMoney(row.getCell(11), evaluator));
				detail.setRemark(getStringValue(row.getCell(15), evaluator));
				detail.setSpjangcd("ZZ");
				detail.setPurchasedt(misdate);

				TB_SalesDetailId id = new TB_SalesDetailId();
				id.setMisnum(saved.getMisnum());
				String misseq2 = String.format("%03d", misseq++).trim();
				if(misseq2.length() > 3){
					misseq2 = misseq2.substring(0, 3);
					System.out.println("[WARN] misseq 잘림 → " + misseq2);
				}
				id.setMisseq(misseq2);
				detail.setId(id);

				detail.setSalesment(saved);
				salesDetailRepository.save(detail);

			}

			result.message = "엑셀데이터 삽입 완료";

		} catch (Exception e) {
			e.printStackTrace();
			result.message = "엑셀 처리 중 오류 발생: " + e.getMessage();
		}

		return result;
	}

	// ================= 헬퍼 =================

	private String getDateString(Cell cell) {
		if (cell == null) return null;
		try {
			if (DateUtil.isCellDateFormatted(cell)) {
				java.util.Date date = cell.getDateCellValue();
				return new java.text.SimpleDateFormat("yyyyMMdd").format(date);
			} else {
				String dateStr = cell.toString().trim().replace("-", "");
				return dateStr.isEmpty() ? null : dateStr;
			}
		} catch (Exception e) {
			return null;
		}
	}

	private String truncate(String val, int max) {
		if (val == null) return null;
		return val.length() > max ? val.substring(0, max) : val;
	}

	// ✅ 문자열 (수식 포함)
	private String getStringValue(Cell cell, FormulaEvaluator evaluator) {
		if (cell == null) return null;
		CellValue cv = evaluator.evaluate(cell);
		if (cv == null) return null;

		switch (cv.getCellType()) {
			case STRING:
				return cv.getStringValue().trim();
			case NUMERIC:
				return String.valueOf((int) cv.getNumberValue());
			case BOOLEAN:
				return String.valueOf(cv.getBooleanValue());
			default:
				return null;
		}
	}

	// ✅ 정수형 (수식 포함)
	private Integer getIntegerValue(Cell cell, FormulaEvaluator evaluator) {
		if (cell == null) return null;
		CellValue cv = evaluator.evaluate(cell);
		if (cv == null) return null;

		try {
			if (cv.getCellType() == CellType.NUMERIC) {
				return (int) Math.round(cv.getNumberValue());
			} else if (cv.getCellType() == CellType.STRING) {
				String s = cv.getStringValue().trim();
				if (s.isEmpty()) return null;
				return (int) Double.parseDouble(s);
			}
		} catch (Exception e) {
			return null;
		}
		return null;
	}

	// ✅ 금액 셀 (수식 포함, ₩/쉼표 제거)
	private Integer getIntMoney(Cell cell, FormulaEvaluator evaluator) {
		if (cell == null) return null;
		CellValue cv = evaluator.evaluate(cell);
		if (cv == null) return null;
		try {
			if (cv.getCellType() == CellType.NUMERIC) {
				return (int) Math.round(cv.getNumberValue());
			} else if (cv.getCellType() == CellType.STRING) {
				String val = cv.getStringValue().trim().replaceAll("[,\\s₩원]", "");
				if (val.isEmpty()) return null;
				return (int) Math.round(Double.parseDouble(val));
			}
		} catch (Exception e) {
			return null;
		}
		return null;
	}
	// 세금계산서 저장
	@PostMapping("/invoice_save")
	public AjaxResult saveInvoice(@RequestBody Map<String, Object> form
	, Authentication auth) {
		User user = (User) auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		String invoiceeType = (String) form.get("InvoiceeType");

		if ("사업자".equals(invoiceeType)) {

			// 1. InvoiceeID 없는 경우 → 사업자번호로 조회
			String corpNum = (String) form.get("InvoiceeCorpNum");

			// 2. 사업자번호 유효성 체크
			if (salesInvoiceService.validateSingleBusiness(corpNum) == null) {
				result.success = false;
				result.message = "휴/폐업 사업자번호입니다.\n공급받는자 등록번호를 확인해주세요.";
				return result;
			}

			// 3. company 테이블에 존재 확인
			Optional<Company> comp = companyRepository.findByBusinessNumber(corpNum);
			if (comp.isPresent()) {
				form.put("InvoiceeID", comp.get().getId());
			} else {
				// 4. 없으면 신규 등록
				AjaxResult compResult = salesInvoiceService.saveInvoicee(form, user);
				if (!compResult.success) {
					return compResult; // 에러 바로 리턴
				}
				Company newComp = (Company) compResult.data;
				form.put("InvoiceeID", newComp.getId());
			}
		}


		// 수정세금계산서 신규 생성
		if ("true".equals(String.valueOf(form.get("newModifiedInvoice")))) {
			return salesInvoiceService.saveModifiedInvoice(form);
		} else {
			return salesInvoiceService.saveInvoice(form);
		}
	}

	@PostMapping("/invoice_update")
	public AjaxResult updateInvoice(@RequestParam(value="misnum") Integer misnum,
									@RequestParam(value="issuediv") String issuediv,
									Authentication auth) {

		return salesInvoiceService.updateinvoice(misnum, issuediv);
	}

	@PostMapping("/save_invoice_pdf")
	public ResponseEntity<?> saveInvoicePdf(@RequestBody Map<String, String> body) {
		String html = body.get("html");
		String misnum = body.get("misnum");

		String basePath = settings.getProperty("file_temp_upload_path");
		if (basePath == null) {
			return ResponseEntity.status(500).body("PDF 경로 설정이 누락되었습니다.");
		}

		File dir = new File(basePath);
		if (!dir.exists()) dir.mkdirs();

		String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		String filePath = basePath + "TAX-" + today + "-" + misnum + ".pdf";

		try (OutputStream os = new FileOutputStream(filePath)) {
			PdfRendererBuilder builder = new PdfRendererBuilder();

			// Windows에서 맑은 고딕 폰트 등록
			builder.useFont(
					new File("C:/Windows/Fonts/malgun.ttf"),
					"Malgun Gothic",
					400,
					BaseRendererBuilder.FontStyle.NORMAL,
					true
			);

			builder.withHtmlContent(html, null);
			builder.toStream(os);
			builder.run();
			os.flush();

		} catch (Exception e) {
			return ResponseEntity.status(500).body("PDF 생성 실패");
		}

		return ResponseEntity.ok(Map.of("success", true, "message", "PDF 생성 완료"));

	}


	@PostMapping("/invoice_issue")
	public AjaxResult issueInvoice(@RequestBody List<Map<String, String>> issueList) {

		return salesInvoiceService.issueInvoice(issueList);
	}

	@GetMapping("/invoice_detail")
	public AjaxResult getInvoiceDetail(
			@RequestParam("misnum") Integer misnum,
			HttpServletRequest request) throws IOException {

		Map<String, Object> item = this.salesInvoiceService.getInvoiceDetail(misnum);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	@PostMapping("/invoice_delete")
	public AjaxResult deleteSalesment(@RequestBody List<Map<String, String>> deleteList) {

		return salesInvoiceService.deleteSalesment(deleteList);
	}

	@PostMapping("/cancel_issue")
	public AjaxResult cancelIssue(@RequestBody List<Map<String, String>> cancelList) {

		return salesInvoiceService.cancelIssue(cancelList);
	}

	@PostMapping("/re_message")
	public AjaxResult reMessage(@RequestBody List<Map<String, String>> invoiceList) {

		return salesInvoiceService.reMessage(invoiceList);
	}

	@PostMapping("/issue_delete")
	public AjaxResult deleteInvoice(@RequestBody List<Map<String, String>> delList) {

		return salesInvoiceService.deleteInvoice(delList);
	}

	@PostMapping("/upload_save")
	public AjaxResult saveInvoiceBulkData(
			@RequestParam(value="upload_file") MultipartFile upload_file,
			String spjangcd, Authentication auth) throws FileNotFoundException, IOException  {

		User user = (User) auth.getPrincipal();
		return salesInvoiceService.saveInvoiceBulkData(upload_file, spjangcd, user);
	}

	@PostMapping("/invoice_copy")
	public AjaxResult copyInvoice(@RequestBody List<Map<String, String>> copyList) {

		return salesInvoiceService.copyInvoice(copyList);
	}

	@GetMapping("/invoice_print")
	public AjaxResult getInvoicePrint(
			@RequestParam("misnum") Integer misnum,
			HttpServletResponse response) throws IOException {

		Map<String, Object> item = this.salesInvoiceService.getInvoicePrint(misnum);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

}
