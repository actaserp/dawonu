package mes.app.transaction;

import mes.app.aop.DecryptField;
import mes.app.transaction.service.PurchaseInvoiceService;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CompanyRepository;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.TB_InvoiceDetailRepository;
import mes.domain.repository.TB_InvoicementRepository;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api/tran/purchase")
public class PurchaseInvoiceController {
	
	@Autowired
	private PurchaseInvoiceService purchaseInvoiceService;
    @Autowired
    private CompanyRepository companyRepository;
    @Autowired
    private MaterialRepository materialRepository;

	@Autowired
	private TB_InvoicementRepository invoicementRepository;

	@Autowired
	private TB_InvoiceDetailRepository invoiceDetailRepository;

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

	// 검색
	@DecryptField(columns = {"incardnum", "paycltnm", "cltnm"}, masks = {0, 0, 0})
	@GetMapping("/read")
	public AjaxResult getInvoiceList(
			@RequestParam(value="invoice_kind", required=false) String invoice_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="cboCompany", required=false) Integer cboCompany,
			@RequestParam(value="spjangcd", required=false) String spjangcd,
			HttpServletRequest request) {

		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.purchaseInvoiceService.getList(invoice_kind, cboCompany, start, end, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 세금계산서 저장
	@PostMapping("/invoice_save")
	public AjaxResult saveInvoice(@RequestBody Map<String, Object> form
	, Authentication auth) {

		return purchaseInvoiceService.saveInvoice(form);
	}

	@PostMapping("/invoice_update")
	public AjaxResult updateInvoice(@RequestParam(value="misnum") Integer misnum,
									@RequestParam(value="issuediv") String issuediv,
									Authentication auth) {

		return purchaseInvoiceService.updateinvoice(misnum, issuediv);
	}

	@GetMapping("/invoice_detail")
	public AjaxResult getInvoiceDetail(
			@RequestParam("misnum") Integer misnum,
			HttpServletRequest request) throws IOException {

		Map<String, Object> item = this.purchaseInvoiceService.getInvoiceDetail(misnum);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}


	//초기 데이터 삽입용
	// ✅ 초기 데이터 삽입용
	@GetMapping("/read_excel")
	@Transactional
	public AjaxResult setInitData() {
		String filePath = "C:\\Users\\User\\Desktop\\신우\\invoicement_data.xls";
		AjaxResult result = new AjaxResult();

		try (FileInputStream fis = new FileInputStream(filePath);
			 Workbook workbook = new HSSFWorkbook(fis)) {

			Sheet sheet = workbook.getSheet("매입");
			if (sheet == null) {
				result.message = "시트 '매입'을 찾을 수 없습니다.";
				return result;
			}

			int misseq = 1; // misseq 번호용

			for (int i = 1; i <= sheet.getLastRowNum(); i++) {
				Row row = sheet.getRow(i);
				if (row == null) continue;

				// ✅ 1️⃣ 부모 인보이스 생성
				TB_Invoicement tb = new TB_Invoicement();

				Cell cellC = row.getCell(2);
				String misdate = null;
				if (cellC != null) {
					if (DateUtil.isCellDateFormatted(cellC)) {
						java.util.Date date = cellC.getDateCellValue();
						misdate = new java.text.SimpleDateFormat("yyyyMMdd").format(date);
					} else {
						String dateStr = cellC.toString().trim().replace("-", "");
						misdate = dateStr.isEmpty() ? null : dateStr;
					}
					tb.setMisdate(misdate);
				}

				Integer cltcd = getIntegerValue(row.getCell(4));
				tb.setCltcd(cltcd);
				tb.setPaycltcd(cltcd);
				tb.setSupplycost(getIntMoney(row.getCell(9)));
				tb.setTaxtotal(getIntMoney(row.getCell(10)));
				tb.setTotalamt(getIntMoney(row.getCell(11)));
				tb.setTitle(truncate(getStringValue(row.getCell(8)), 100));
				tb.setRemark1(truncate(getStringValue(row.getCell(15)), 1000));
				tb.setSpjangcd("ZZ");
				tb.setMisgubun("purchase_tax");
				tb.setCltflag("0");
				tb.setPaycltflag("0");

				// ✅ 2️⃣ 부모 먼저 save (misnum 생성됨)
				TB_Invoicement saved = invoicementRepository.save(tb);

				// ✅ 3️⃣ 자식 디테일 생성
				TB_InvoiceDetail detail = new TB_InvoiceDetail();
				detail.setMisdate(misdate);
				detail.setItemnm(getStringValue(row.getCell(8)));
				detail.setSupplycost(getIntMoney(row.getCell(9)));
				detail.setTaxtotal(getIntMoney(row.getCell(10)));
				detail.setTotalamt(getIntMoney(row.getCell(11)));
				detail.setRemark(getStringValue(row.getCell(15)));
				detail.setSpjangcd("ZZ");
				detail.setPurchasedt(misdate);

				// ✅ 4️⃣ 복합키 직접 세팅 (부모 misnum 포함)
				TB_InvoiceDetailId id = new TB_InvoiceDetailId();
				id.setMisnum(saved.getMisnum());
				String misseq2 = String.format("%03d", misseq++).trim();
				if (misseq2.length() > 3) {
					misseq2 = misseq2.substring(0, 3); // 안전 자르기
					System.out.println("[WARN] misseq 잘림 → " + misseq2);
				}
				id.setMisseq(misseq2);
				detail.setId(id);

				// ✅ 5️⃣ 연관관계 및 저장
				detail.setInvoicement(saved);
				invoiceDetailRepository.save(detail);
			}

			result.message = "엑셀 데이터 삽입 완료";
		} catch (Exception e) {
			e.printStackTrace();
			result.message = "엑셀 처리 중 오류 발생: " + e.getMessage();
		}

		return result;
	}

// ================= 헬퍼 메서드 =================

	private String truncate(String val, int max) {
		if (val == null) return null;
		return val.length() > max ? val.substring(0, max) : val;
	}

	private String getStringValue(Cell cell) {
		if (cell == null) return null;
		cell.setCellType(CellType.STRING);
		String value = cell.getStringCellValue().trim();
		return value.isEmpty() ? null : value;
	}

	private Integer getIntegerValue(Cell cell) {
		if (cell == null) return null;
		try {
			return (int) Double.parseDouble(cell.toString().trim());
		} catch (Exception e) {
			return null;
		}
	}

	private Integer getIntMoney(Cell cell) {
		if (cell == null) return null;
		try {
			String val = cell.toString().trim().replaceAll("[,\\s₩원]", "");
			if (val.isEmpty()) return null;
			double parsed = Double.parseDouble(val);
			return (int) Math.round(parsed);
		} catch (Exception e) {
			return null;
		}
	}

	@PostMapping("/invoice_delete")
	public AjaxResult deleteSalesment(@RequestBody List<Map<String, String>> deleteList) {

		return purchaseInvoiceService.deleteInvoicement(deleteList);
	}

	@PostMapping("/invoice_copy")
	public AjaxResult copyInvoice(@RequestBody List<Map<String, String>> copyList) {

		return purchaseInvoiceService.copyInvoice(copyList);
	}

}
