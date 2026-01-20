package mes.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
public class BomBuildReport {

    private final String rootCode;

    // 생성된 BOM (code → bomId)
    private final Map<String, Long> bomMap = new LinkedHashMap<>();

    // BOM 구성품 (부모 → 자식 목록)
    private final Map<String, List<BomCompInfo>> bomCompMap = new LinkedHashMap<>();

    // 스킵된 항목 (code → 사유)
    private final Map<String, String> skipped = new LinkedHashMap<>();

    // 더 내려가지 않는 leaf 노드
    private final List<String> leafNodes = new ArrayList<>();

    public BomBuildReport(String rootCode) {
        this.rootCode = rootCode;
    }

    /* ===================== 기록 메소드 ===================== */

    public void addBom(String code, Long bomId) {
        bomMap.put(code, bomId);
    }

    public void addComp(String parentCode, String compCode, int amount) {
        bomCompMap
                .computeIfAbsent(parentCode, k -> new ArrayList<>())
                .add(new BomCompInfo(compCode, amount));
    }

    public void addSkip(String code, String reason) {
        skipped.put(code, reason);
    }

    public void addLeaf(String code) {
        leafNodes.add(code);
    }

    /* ===================== 내부 DTO ===================== */

    @Getter
    @AllArgsConstructor
    public static class BomCompInfo {
        private String code;
        private int amount;
    }
}
