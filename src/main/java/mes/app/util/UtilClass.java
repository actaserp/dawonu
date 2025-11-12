package mes.app.util;

import lombok.extern.slf4j.Slf4j;
import mes.Encryption.EncryptionKeyProvider;
import mes.Encryption.EncryptionUtil;
import mes.domain.model.AjaxResult;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.Map;
import java.util.List;
import java.util.Optional;

@Slf4j
public class UtilClass {

    public static Integer getInt(Map<String, Object> map, String key) {
        if (map == null || key == null) return null;

        Object val = map.get(key);
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }

        return null;
    }

    public static Integer parseInteger(Object obj){
        if(obj == null) return null;
        if (obj instanceof Integer) return (Integer) obj;

        try{
            return Integer.parseInt(obj.toString());
        }catch (NumberFormatException e){
            return null;
        }
    }

    public static boolean isValidDate(String yyymmdd){
        try{
            LocalDate.parse(yyymmdd, DateTimeFormatter.ofPattern("yyyyMMdd"));

            return true;
        }catch(DateTimeParseException e){
            return false;
        }
    }

    /**
     * @Return : yyyyMMddHHmmss
     */
    public static String combineDateAndHourReturnyyyyMMddHHmmss(String date, String time){
        try{

            if(time == null || time.isBlank()){
                time = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
            }

            String combined = date + " " + time;

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
            LocalDateTime dateTime = LocalDateTime.parse(combined, formatter);

            DateTimeFormatter output = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            return dateTime.format(output);

        }catch (DateTimeParseException e){
            throw new IllegalArgumentException("날짜/시간 형식이 올바르지 않습니다.");
        }
    }

    /**
     * 20250901 요따구로 들어오묜 2025-09-01 로 반환해주는 헬퍼 메서드
     * @Return : yyyy-MM-dd (String)
     * **/
    public static String toContainsHyphenDateString(String yyyymmdd){
        try{
            if(yyyymmdd == null || yyyymmdd.length() != 8){
                log.info("올바르지 않은 형식이 들어옴");
                return "";
            }

            return yyyymmdd.substring(0, 4) + "-" +
                    yyyymmdd.substring(4, 6) + "-" +
                    yyyymmdd.substring(6, 8);

        }catch(Exception e){
            log.info("올바르지 않은 형식이 들어옴");
            return "";
        }
    }
    /***
     *  오늘날짜에서 param으로 들어온 값만큼 빼서 반환
     * **/
    public static String getDayByParamAdd(int day){

        LocalDate date = LocalDate.now().plusDays(day);

        return date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
    /**
     * 세션에서 사업장 코드를 통해 사업자 번호를 추출하는 메서드
     * ***/
    public static Map<String, Object> getSpjanInfoFromSession(String spjangcd, HttpSession httpSession){
        List<Map<String, Object>> spjangList = (List<Map<String, Object>>) httpSession.getAttribute("spjangList");

        if(spjangList == null) return null;

        for(Map<String, Object> item : spjangList){
            if(spjangcd.equals(item.get("spjancd"))){
                return item;
            }
        }
        return null;
    }
    /***
     * 세션에서 사업자번호 추출하는 메서드
     * **/
    public static String getsaupnumInfoFromSession(String spjangcd, HttpSession httpSession){
        List<Map<String, Object>> spjangList = (List<Map<String, Object>>) httpSession.getAttribute("spjangList");

        if(spjangList == null) return null;

        for(Map<String, Object> item : spjangList){
            if(spjangcd.equals(item.get("spjangcd"))){
                return String.valueOf(item.get("saupnum"));
            }
        }
        return null;
    }

    /**
     * 객체를 안전하게 문자열로 변환한다.
     * - null일 경우 빈 문자열("") 반환
     * - null이 아니면 toString() 결과 반환
     */
    public static String getStringSafe(Object obj) {
        return obj == null ? "" : obj.toString().trim();
    }

    /**
     * 요소를 순회하며 지정된 컬럼의 암호화된 값들을 복호화한 뒤 일부 자릿수를 마스킹 처리해 덮어씌우는 메서드
     *
     * @param list         암호화된 값을 가진 Map 객체들의 리스트
     * @param col          복호화 및 마스킹할 컬럼 이름
     * @param maskLength   복호화 후 마스킹할 길이 (끝에서 몇 자리 마스킹할지 지정)
     * @throws Exception   복호화 도중 예외 발생 시
     */
    public static void decryptEachItem(List<Map<String, Object>> list, String col, int maskLength) throws IOException {
        byte[] key = EncryptionKeyProvider.getKey();

        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> item = list.get(i);
            Object encrypt = item.get(col);
            String parsedEncrypt = encrypt != null ? encrypt.toString() : "";

            if (!parsedEncrypt.isEmpty()) {
                try {
                    // 복호화 시도
                    String decrypted = EncryptionUtil.decrypt(parsedEncrypt, key);
                    String masked = applyMasking(decrypted, maskLength);
                    item.put(col, masked);
                } catch (Exception e) {
                    // 복호화 불가능한 값이므로 평문으로 간주, 그대로 유지
                }
            }
        }
    }

    public static void decryptItem(Map<String, Object> map, String col, int maskLength) throws IOException {
        byte[] key = EncryptionKeyProvider.getKey();

        Object encryptedValue = map.get(col);
        String parsedEncrypt = encryptedValue != null ? encryptedValue.toString() : "";

        if (!parsedEncrypt.isEmpty()) {
            try {
                // 복호화
                String decrypted = EncryptionUtil.decrypt(parsedEncrypt, key);

                // 마스킹
                String masked = applyMasking(decrypted, maskLength);

                // 결과 반영
                map.put(col, masked);
            } catch (Exception e) {
                // 복호화 불가 → 그대로 유지
            }
        }
    }


    /**
     * 문자열의 끝에서부터 지정된 길이만큼 마스킹 처리하는 메서드
     *
     * @param input       원본 문자열
     * @param maskLength  마스킹할 길이
     * @return 마스킹된 문자열
     */
    private static String applyMasking(String input, int maskLength) {
        if (input == null || input.length() <= maskLength) {
            return "⋆".repeat(Math.max(0, input.length())); // 전체 마스킹
        }
        int visibleLength = input.length() - maskLength;
        return input.substring(0, visibleLength) + "⋆".repeat(maskLength);
    }

    /**
     * standard 컬럼 (suju 테이블) 이 varchar라서 여러가지 값이 오는데
     * 값들을 파싱해주는거. 캐스팅 불가하면 1로 리턴
     * **/
    public static double parseStandard(Object value){
        if(value == null) return 1.0;
        try{
            if(value instanceof Number) return ((Number) value).doubleValue();
            String str = value.toString().trim();
            if(str.isEmpty()) return 1.0;
            return Double.parseDouble(str);
        }catch (Exception e){
            return 1.0;
        }
    }

    /**
     * 은행명에 따라 계좌번호 하이픈(-) 자동 포맷팅
     * @param accnum  복호화된 계좌번호 (숫자 문자열)
     * @param banknm  은행명 (예: 국민은행, 신한은행 등)
     * @return 포맷팅된 계좌번호 (예: 123-456-789012)
     */
    public static String bankFormat(String accnum, String banknm) {
        if (accnum == null || accnum.isEmpty()) return "";
        if (banknm == null) banknm = "";

        // 숫자만 추출
        accnum = accnum.replaceAll("\\D", "");

        switch (banknm.trim()) {

            // ✅ 국민은행: 14자리 (6-2-6)
            case "국민은행":
                if (accnum.length() == 14)
                    return accnum.replaceFirst("(\\d{6})(\\d{2})(\\d{6})", "$1-$2-$3");
                break;

            // ✅ 신한은행: 12자리 (3-3-6)
            case "신한은행":
                if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{3})(\\d{3})(\\d{6})", "$1-$2-$3");
                break;

            // ✅ 농협: 14자리 (3-4-6-1)
            case "농협":
            case "농협은행":
                if (accnum.length() == 13)
                    return accnum.replaceFirst("(\\d{3})(\\d{4})(\\d{4})(\\d{2})", "$1-$2-$3-$4");
                else if (accnum.length() == 14)
                    return accnum.replaceFirst("(\\d{3})(\\d{4})(\\d{6})(\\d{1})", "$1-$2-$3-$4");
                else if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{3})(\\d{3})(\\d{6})", "$1-$2-$3");
                break;

            // ✅ 우리은행: 13자리 (8-5)
            case "우리은행":
                if (accnum.length() == 13)
                    return accnum.replaceFirst("(\\d{8})(\\d{5})", "$1-$2");
                break;

            // ✅ 하나은행: 12자리 (3-6-3)
            case "하나은행":
                if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{3})(\\d{6})(\\d{3})", "$1-$2-$3");
                break;

            // ✅ 기업은행: 13자리 (3-6-4)
            case "기업은행":
                if (accnum.length() == 13)
                    return accnum.replaceFirst("(\\d{3})(\\d{6})(\\d{4})", "$1-$2-$3");
                else if (accnum.length() == 14)
                    return accnum.replaceFirst("(\\d{3})(\\d{6})(\\d{2})(\\d{3})", "$1-$2-$3-$4");
                break;

            // ✅ SC제일은행: 11~12자리 (3-2-6 or 3-3-6)
            case "SC제일은행":
                if (accnum.length() == 11)
                    return accnum.replaceFirst("(\\d{3})(\\d{2})(\\d{6})", "$1-$2-$3");
                else if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{3})(\\d{3})(\\d{6})", "$1-$2-$3");
                break;

            // ✅ 씨티은행: 10~12자리 (3-6-3)
            case "씨티은행":
                if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{3})(\\d{6})(\\d{3})", "$1-$2-$3");
                break;

            // ✅ 산업은행: 12자리 (3-3-6)
            case "산업은행":
                if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{3})(\\d{3})(\\d{6})", "$1-$2-$3");
                break;

            // ✅ 카카오뱅크: 11자리 (333-12-345678)
            case "카카오뱅크":
                if (accnum.length() == 11)
                    return accnum.replaceFirst("(\\d{3})(\\d{2})(\\d{6})", "$1-$2-$3");
                break;

            // ✅ 토스뱅크: 12자리 (100-1234-567890)
            case "토스뱅크":
                if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{3})(\\d{4})(\\d{5})", "$1-$2-$3");
                break;

            // ✅ 새마을금고: 13자리 (9-4)
            case "새마을금고":
                if (accnum.length() == 13)
                    return accnum.replaceFirst("(\\d{9})(\\d{4})", "$1-$2");
                break;

            // ✅ 신협: 12~13자리 (4-4-4 or 4-5-4)
            case "신협":
                if (accnum.length() == 12)
                    return accnum.replaceFirst("(\\d{4})(\\d{4})(\\d{4})", "$1-$2-$3");
                else if (accnum.length() == 13)
                    return accnum.replaceFirst("(\\d{4})(\\d{5})(\\d{4})", "$1-$2-$3");
                break;

            // ✅ 우체국: 13자리 (6-2-5)
            case "우체국":
                if (accnum.length() == 13)
                    return accnum.replaceFirst("(\\d{6})(\\d{2})(\\d{5})", "$1-$2-$3");
                break;

            default:
                return accnum; // 은행명 매칭 안 되면 원본 그대로
        }

        return accnum;
    }

}
