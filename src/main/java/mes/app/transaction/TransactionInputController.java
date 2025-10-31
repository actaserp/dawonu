package mes.app.transaction;


import lombok.extern.slf4j.Slf4j;
import mes.app.aop.DecryptField;
import mes.app.transaction.service.TransactionInputService;
import mes.app.util.UtilClass;
import mes.domain.dto.BankTransitDto;
import mes.domain.entity.TB_BANKTRANSIT;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TB_BANKTRANSITRepository;
import mes.domain.services.SqlRunner;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import javax.validation.Valid;
import java.io.FileInputStream;
import java.util.*;

@RestController
@RequestMapping("/api/transaction/input")
@Slf4j
public class TransactionInputController {
    @Autowired
    private TB_BANKTRANSITRepository tB_BANKTRANSITRepository;

    @Autowired
    TransactionInputService transactionInputService;

    @Autowired
    DataSource dataSource;

    @Autowired
    SqlRunner sqlRunner;


    @DecryptField(columns = {"accountnumber", "paymentpw", "onlinebankpw", "viewpw"}, masks = {0, 2, 2, 2})
    @GetMapping("/registerAccount")
    public AjaxResult registerAccount(@RequestParam String spjangcd){

        AjaxResult result = new AjaxResult();

        result.data = transactionInputService.getAccountList(spjangcd);

        Map<String, Object> status = new LinkedHashMap<>();


        return  result;

    }

    @DecryptField(columns = {"account", "clientName"}, masks = 0)
    @GetMapping("/history")
    public AjaxResult TransactionHistory(@RequestParam String searchfrdate,
                                         @RequestParam String searchtodate,
                                         @RequestParam String tradetype,
                                         @RequestParam String spjangcd,
                                         @RequestParam(required = false) String accountNameHidden,
                                         @RequestParam(required = false) String cltflag,
                                         @RequestParam(required = false) String cboCompanyHidden) throws InterruptedException {
        long start = System.currentTimeMillis();

        AjaxResult result = new AjaxResult();

        searchfrdate = searchfrdate.replaceAll("-", "");
        searchtodate = searchtodate.replaceAll("-", "");

        Integer parsedAccountId = null;
        if(accountNameHidden != null && !accountNameHidden.isEmpty()){
            parsedAccountId = UtilClass.parseInteger(accountNameHidden);
        }

        Integer parsedCompanyId = null;
        if(cboCompanyHidden != null && !cboCompanyHidden.isEmpty()){
            parsedCompanyId = UtilClass.parseInteger(cboCompanyHidden);
        }

        Map<String, Object> param = new HashMap<>();

        param.put("searchfrdate", searchfrdate);
        param.put("searchtodate", searchtodate); // searchtodate가 null이더라도 문제없이 추가됨
        param.put("parsedAccountId", parsedAccountId);
        param.put("parsedCompanyId", parsedCompanyId);
        param.put("spjangcd", spjangcd);
        param.put("cltflag", cltflag);
        param.put("tradetype", tradetype);

        result.data = transactionInputService.getTransactionHistory(param);


        long end = System.currentTimeMillis();
        System.out.println("끝남시간: " + end);
        System.out.println("[/history] 처리 시간: " + (end - start) + " ms");
        return result;
    }

    @PostMapping("/transactionForm")
    public AjaxResult transactionForm(@Valid @RequestBody BankTransitDto data,
                                      BindingResult bindingResult){

        AjaxResult result = new AjaxResult();

        if(bindingResult.hasErrors()){
            result.success = false;
            result.message = bindingResult.getAllErrors().get(0).getDefaultMessage();
            return result;
        }
        try{
            transactionInputService.saveBankTransit(data);
            result.success = true;
            result.message = "저장하였습니다.";
        }catch(Exception e){
            result.success = false;
            result.message = "오류가 발생하였습니다.";

            return result;
        }

        return  result;
    }

    @PostMapping("/AccountEdit")
    public AjaxResult AccountEdit(@RequestBody Object list){

        AjaxResult result = new AjaxResult();

        try {
            transactionInputService.editAccountList((List<Map<String, Object>>)list);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        result.message = "수정되었습니다.";
        return  result;

    }

    @PostMapping("/edit")
    public AjaxResult transactionEdit(@RequestBody Object list){

        AjaxResult result = transactionInputService.editBankTransit(list);
        return result;
    }

    @DecryptField(columns = {"accountNumber", "clientName"}, masks = 0)
    @GetMapping("/searchDetail")
    public AjaxResult searchTransactionDetail(@RequestParam String companyId,
                                              @RequestParam String cltflag,
                                              @RequestParam String searchfrdate,
                                              @RequestParam String searchtodate,
                                              @RequestParam String spjangcd){
        AjaxResult result = new AjaxResult();

        searchfrdate = searchfrdate.replaceAll("-", "");
        searchtodate = searchtodate.replaceAll("-", "");

        result.data = transactionInputService.searchDetail(UtilClass.parseInteger(companyId), cltflag, searchfrdate,  searchtodate, spjangcd);

        return  result;
    }

    @PostMapping("/delete")
    public AjaxResult transactionDelete(@RequestParam String idList){

        AjaxResult result = new AjaxResult();

        List<Integer> parsedidList = Arrays.stream(idList.split(","))
                        .map(Integer::parseInt)
                                .toList();

        transactionInputService.deleteBanktransit(parsedidList);

        result.message = "삭제되었습니다.";
        return  result;

    }

    //초기데이터 입고용
    @GetMapping("/read_excel")
    @Transactional
    public AjaxResult setInitData3(){
//        String filePath = "C:\\Users\\User\\Desktop\\신우\\collection_money.xls";
        String filePath = "C:\\Users\\User\\Desktop\\신우\\withdraw_money.xls";

        AjaxResult result = new AjaxResult();

        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = new HSSFWorkbook(fis)){

            // ✅ 수식 평가기 생성
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

            //Sheet sheet = workbook.getSheet("수금");
            /*if (sheet == null) {
                result.message = "시트 '수금'을 찾을 수 없습니다.";
                return result;
            }*/

            Sheet sheet = workbook.getSheet("지급");
            if(sheet == null){
                result.message = "시트 '지급'을 찾을 수 없습니다.";
                return result;
            }

            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                TB_BANKTRANSIT tb = new TB_BANKTRANSIT();

                tb.setIoflag("1");
                tb.setTrdate(getDateString(row.getCell(2))); //거래일자
                tb.setTrdt(getDateTimeString(row.getCell(2)));

                tb.setSupplyamt(getIntMoney(row.getCell(9), evaluator));
                tb.setTax(getIntMoney(row.getCell(10), evaluator));

                //tb.setAccin(getIntMoney(row.getCell(11), evaluator)); //입금액
                tb.setAccout(getIntMoney(row.getCell(11), evaluator)); //출금액

                tb.setRegdt(getDateTimeString(row.getCell(2))); //등록일시
                tb.setRegpernm("관리자"); //
                tb.setMemo(truncate(getStringValue(row.getCell(15), evaluator), 1000));
                tb.setCltcd(getIntegerValue(row.getCell(4), evaluator));
                tb.setTrid(29);

                String iotype = "";
                String ioCell = getStringValue(row.getCell(13), evaluator);

                if(ioCell != null){
                    if(ioCell.equals("받을어음")){
                        iotype = "7";
                    }else if(ioCell.equals("보통예금")){
                        iotype = "0";
                    }else if(ioCell.equals("상계")){
                        iotype = "8";
                    }else if(ioCell.equals("지급어음")){
                        iotype = "9";
                    }else if(ioCell.equals("현금")){
                        iotype = "1";
                    }
                }


                tb.setIotype(iotype); //입금형태
                tb.setPaymentnum(getStringValue(row.getCell(14), evaluator));
                tb.setSpjangcd("ZZ");
                tb.setCltflag("0");

                tB_BANKTRANSITRepository.save(tb);
            }

            result.message = "엑셀데이터 삽입 완료";
        }catch(Exception e){
            e.printStackTrace();
            result.message = "엑셀 처리 중 오류 발생: " + e.getMessage();
        }
        return result;
    }


    //마감 기초 입고 (미지급, 미수금)
    @GetMapping("/read_excel2")
    @Transactional
    public AjaxResult setInitData4(){
        AjaxResult result = new AjaxResult();

        String filePath = "C:\\Users\\User\\Desktop\\신우\\미수미지급총잔액.xls";

        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = new HSSFWorkbook(fis)){

            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

            Sheet sheet = workbook.getSheet("Sheet1");
            if(sheet == null){
                result.message = "시트 'Sheet1'을 찾을 수 없습니다.";
                return result;
            }
            Map<String, Map<String, Object>> dataSet = new HashMap<>();


            for(int i = 1; i <= sheet.getLastRowNum(); i++){
                Row row = sheet.getRow(i);
                if (row == null) continue;

                Integer cltcd = getIntegerValue(row.getCell(0), evaluator);
                Integer amt     = getIntegerValue(row.getCell(6), evaluator);

                String ioflag = "";

                if(amt==null){
                    System.out.println("amt null");
                }

                if(amt<0){
                    ioflag = "1"; //출금이니깐 -> 내 통장에서 나가는거 (미지급)
                }else if(amt > 0){
                    ioflag = "0"; //입금이니깐 -> 내 통장으로 들어오는거 (미수금)
                }else{
                    throw new RuntimeException("뭐해");
                }

                if(ioflag.isEmpty()){
                    throw new RuntimeException("플래그 값 알수 없는데");
                }

                MapSqlParameterSource param = new MapSqlParameterSource();
                param.addValue("cltcd", cltcd);
                param.addValue("ioflag", ioflag);
                param.addValue("yearamt", amt);

                String sql = """
                        insert into tb_yearamt
                        (ioflag, yyyymm, cltcd, yearamt, endyn, spjangcd, vercode)
                        values
                        (
                        :ioflag, '202212', :cltcd, :yearamt, 'Y', 'ZZ', 0
                        )
                        """;

                this.sqlRunner.execute(sql, param);

            }

        }catch(Exception e){
            e.printStackTrace();
            result.message = "엑셀 처리 중 오류 발생: " + e.getMessage();
        }
        return result;

    }

    // ================= 헬퍼 =================

    private String getDateTimeString(Cell cell) {
        if (cell == null) return null;
        try {
            if (DateUtil.isCellDateFormatted(cell)) {
                java.util.Date date = cell.getDateCellValue();

                // 현재 시각 기준으로 시분초 붙이려면 ↓
                java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyyMMdd");
                java.text.SimpleDateFormat timeFormat = new java.text.SimpleDateFormat("HHmmss");

                String datePart = dateFormat.format(date);
                String timePart = timeFormat.format(new java.util.Date()); // 현재 시각 사용

                return datePart + timePart;
            } else {
                // 셀 내용이 문자열일 경우
                String val = cell.toString().trim().replaceAll("[^0-9]", "");
                if (val.length() == 8) {
                    // yyyyMMdd 형태면 현재 시각 붙이기
                    java.text.SimpleDateFormat timeFormat = new java.text.SimpleDateFormat("HHmmss");
                    return val + timeFormat.format(new java.util.Date());
                } else if (val.length() >= 14) {
                    // 이미 yyyyMMddHHmmss 형태면 그대로 사용
                    return val.substring(0, 14);
                } else {
                    return null;
                }
            }
        } catch (Exception e) {
            return null;
        }
    }

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

}
