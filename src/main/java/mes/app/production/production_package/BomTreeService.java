package mes.app.production.production_package;

import mes.Exception.CustomException;

import java.math.BigDecimal;
import java.util.*;

public class BomTreeService {

    /**
     * 1. BOM row → Tree 구조 생성
     */
    public Map<String, BomNode> buildTree(List<Map<String, Object>> bomList) {
        Map<Integer, BomNode> nodeMap = new HashMap<>();

        for (Map<String, Object> row : bomList) {
            BomNode node = new BomNode(row);
            nodeMap.put(node.myKey, node);
        }

        Map<String, BomNode> rootMap = new HashMap<>();

        for (BomNode node : nodeMap.values()) {
            if (node.parentKey == null) {

                boolean hasFirstProcess = !node.class1.isEmpty();

                String key = hasFirstProcess ? "FIRST" : "SINGLE";

                rootMap.put(key, node);

            } else {
                BomNode parent = nodeMap.get(node.parentKey);
                if (parent != null) parent.children.add(node);
            }
        }
        return rootMap;
    }

    /**
     * 2. 공정 기준으로 사용할 자재 선택
     */
    public BomNode selectTargetMaterial(ProcessFlow flow, Map<String, BomNode> bomRoots, Float orderQty) {


        BigDecimal orderQtyBd = BigDecimal.valueOf(orderQty.doubleValue());


        if (flow.startType() == ProcessType.FIRST_CONTAINS) {

            //1차 공정
            BomNode first = bomRoots.get("FIRST");
            BomNode target = findFirstProcessTarget(first, orderQtyBd);

            if(target == null) throw new CustomException("첫 공정을 찾지 못했습니다.");

            return target;
        }else{
            //3차 단독 공정
            BomNode single = bomRoots.get("SINGLE");

            single.calculatedBomRatio = single.bomQty != null ? single.bomQty : BigDecimal.ONE;

            return single;
        }
    }

    private BomNode findFirstProcessTarget(BomNode node, BigDecimal parentRatio){
        if(node == null) throw new CustomException("공정 노드가 존재하지 않습니다.");

        BigDecimal nodeRatio = node.bomQty != null ? node.bomQty : BigDecimal.ONE;
        BigDecimal currentRatio = parentRatio.multiply(nodeRatio);

        boolean class1OK = node.class1 != null && !node.class1.isBlank();
        boolean class2Empty = node.class2 == null || node.class2.isBlank();
        boolean class3Empty = node.class3 == null || node.class3.isBlank();

        if(class1OK && class2Empty && class3Empty){
            node.calculatedBomRatio = currentRatio;
            return node;
        }

        for(BomNode child : node.children){
            BomNode found = findFirstProcessTarget(child, currentRatio);
            if(found != null){
                return found;
            }
        }

        return null;
    }

    private BomNode findFirstProcessMaterial(List<BomNode> roots) {
        BomNode node = roots.get(0);
        while (!node.children.isEmpty()) {
            node = node.children.get(0);
        }
        return node;
    }

    private String random5(){
        return UUID.randomUUID().toString().replace("-", "").substring(0, 5);
    }
}
