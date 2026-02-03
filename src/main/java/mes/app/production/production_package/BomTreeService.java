package mes.app.production.production_package;

import mes.Exception.CustomException;
import mes.app.production.Enum.ProcessType;
import mes.app.production.dto.productionResult.NextContext;
import mes.app.production.service.ProductionResultService;
import mes.app.util.JsonUtil;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;

@Component
public class BomTreeService {

    /**
     * 1. BOM row → Tree 구조 생성
     */
    public Map<String, BomNode> buildTree(List<Map<String, Object>> bomList) {

        Map<Integer, BomNode> nodeMap = new HashMap<>();

        // 1️⃣ row → BomNode
        for (Map<String, Object> row : bomList) {
            BomNode node = new BomNode(row);
            nodeMap.put(node.myKey, node);
        }

        // 2️⃣ 부모-자식 연결
        for (BomNode node : nodeMap.values()) {
            if (node.parentKey != null) {
                BomNode parent = nodeMap.get(node.parentKey);
                if (parent != null) {
                    parent.children.add(node);
                }
            }
        }

        // 3️⃣ 그룹(컨테이너) 루트
        BomNode fullGrp = new BomNode();
        fullGrp.matName = ProcessType.FULL_FLOW.name();

        BomNode simpleGrp = new BomNode();
        simpleGrp.matName = ProcessType.SIMPLE_FLOW.name();

        // 4️⃣ 최상위 공정 분류
        for (BomNode node : nodeMap.values()) {

            if (node.parentKey != null) continue;

            // FULL_FLOW 판단 기준 (의미는 유지, 이름만 명확)
            boolean isFullFlow =
                    node.class3 != null && !node.class3.isEmpty();

            if (isFullFlow) {
                fullGrp.children.add(node);
            } else {
                simpleGrp.children.add(node);
            }
        }

        // 5️⃣ String key rootMap
        Map<String, BomNode> rootMap = new HashMap<>();

        if (!fullGrp.children.isEmpty()) {
            rootMap.put(ProcessType.FULL_FLOW.name(), fullGrp);
        }
        if (!simpleGrp.children.isEmpty()) {
            rootMap.put(ProcessType.SIMPLE_FLOW.name(), simpleGrp);
        }

        return rootMap;
    }


    /**
     * 공정 완료 처리 후 다음 공정 전이 정보 계산
     *
     * - targetMatPk 공정을 complete/current 로 표시
     * - 완료된 공정 기준으로 다음에 진행할 부모 공정(BomNode) 반환
     * - 다음 공정이 없으면 null 반환
     */
    //region: 다음 공정정보 처리및 다음 노드 반환
    public NextContext completeAndGetParentNode(
            Map<String, BomNode> processTree,
            int targetMatPk
    ) {
        processTree.values().forEach(BomNode::clearCursor);

        for (Map.Entry<String, BomNode> entry : processTree.entrySet()) {
            BomNode parent =
                    completeAndFindParent(entry.getValue(), null, targetMatPk);
            if (parent != null) {
                NextContext ctx = new NextContext();
                ctx.setNextNode(parent);
                ctx.setFlowKey(entry.getKey()); // ROOT KEY
                return ctx;
            }
        }
        return null;
    }

    private BomNode completeAndFindParent(
            BomNode current,
            BomNode parent,
            int targetMatPk
    ) {

        if (current.matPk != null && current.matPk == targetMatPk) {



            current.complete = true; //완료 처리

            current.current = true; //현재 공정 표시


            return parent; // 다음 공정
        }

        for (BomNode child : current.children) {
            BomNode found =
                    completeAndFindParent(child, current, targetMatPk);
            if (found != null) {
                return found;
            }
        }

        return null;
    }
    //endregion



    /**
     * BOM 트리 전체를 순회하면서
     * 최초 작업지시 대상 공정들을 current=true 로 마킹한다.
     *
     * - SIMPLE_FLOW : 첫 공정 1개
     * - FULL_FLOW   : (존재 시) class3 기준 병렬 공정들
     */
    public void markFirstCurrentProcess(
            ProcessFlow flow,
            Map<String, BomNode> bomRoots,
            Float orderQty
    ) {
        BigDecimal orderQtyBd = BigDecimal.valueOf(orderQty);

        // 0️⃣ 기존 current 초기화
        bomRoots.values().forEach(BomNode::clearCursor);

        // 1️⃣ 전체 누적 소요량 계산
        for (BomNode root : bomRoots.values()) {
            calculateBomRatioDfs(root, orderQtyBd);
        }

        // =========================
        // 2️⃣ SIMPLE_FLOW (메인 라인)
        // =========================
        BomNode simpleRoot = bomRoots.get(ProcessType.SIMPLE_FLOW.name());
        if (simpleRoot == null) {
            throw new CustomException("SIMPLE_FLOW 루트를 찾을 수 없습니다.");
        }

        BomNode firstSimple =
                findDeepestProcessLeaf(simpleRoot, orderQtyBd);

        if (firstSimple == null) {
            throw new CustomException("첫 공정을 찾지 못했습니다.");
        }

        firstSimple.current = true;

        // =========================
        // 3️⃣ FULL_FLOW (옵션)
        // =========================
        if (flow.hasThirdProcess()) {

            BomNode fullRoot = bomRoots.get(ProcessType.FULL_FLOW.name());

            // FULL_FLOW 는 있을 수도 / 없을 수도 있음
            if (fullRoot != null) {
                markThirdProcessByClass3(fullRoot, orderQtyBd);
            }
        }
    }

    private void calculateBomRatioDfs(
            BomNode node,
            BigDecimal parentQty
    ) {
        if (node == null) return;

        BigDecimal selfQty =
                node.bomQty != null ? node.bomQty : BigDecimal.ONE;

        BigDecimal currentQty = parentQty.multiply(selfQty);
        node.calculatedBomRatio = currentQty;

        for (BomNode child : node.children) {
            calculateBomRatioDfs(child, currentQty);
        }
    }

    private BomNode findDeepestProcessLeaf(
            BomNode root,
            BigDecimal orderQty
    ) {
        BomNode[] best = new BomNode[1];
        int[] bestDepth = new int[]{-1};

        findFirstProcessTarget(
                root,
                orderQty,
                0,
                best,
                bestDepth
        );

        return best[0];
    }

    private void findFirstProcessTarget(
            BomNode node,
            BigDecimal parentRatio,
            int depth,
            BomNode[] best,
            int[] bestDepth
    ) {
        if (node == null) return;

        BigDecimal selfQty =
                node.bomQty != null ? node.bomQty : BigDecimal.ONE;

        BigDecimal currentRatio = parentRatio.multiply(selfQty);
        node.calculatedBomRatio = currentRatio;

        boolean isProcess = isProcessNode(node);
        boolean hasChildProcess = false;

        for (BomNode child : node.children) {
            if (isProcessNode(child)) {
                hasChildProcess = true;
                break;
            }
        }

        // ✅ 공정 리프만 FIRST 후보
        if (isProcess && !hasChildProcess) {
            if (depth > bestDepth[0]) {
                bestDepth[0] = depth;
                best[0] = node;
            }
        }

        for (BomNode child : node.children) {
            findFirstProcessTarget(
                    child,
                    currentRatio,
                    depth + 1,
                    best,
                    bestDepth
            );
        }
    }

    private void markThirdProcessByClass3(
            BomNode node,
            BigDecimal parentRatio
    ) {
        if (node == null) return;

        BigDecimal selfQty =
                node.bomQty != null ? node.bomQty : BigDecimal.ONE;

        BigDecimal currentRatio = parentRatio.multiply(selfQty);

        // ✅ class3 존재 = 3차 병렬 공정
        if (node.class3 != null && !node.class3.isEmpty()) {
            node.calculatedBomRatio = currentRatio;
            node.current = true;
        }

        for (BomNode child : node.children) {
            markThirdProcessByClass3(child, currentRatio);
        }
    }

    /***
     * DB에 저장된 String 타입의 processTree 를 순회하면서
     * 현재 진행하는 material id 와 processTree 에서 materia id 와 일치하는 노드를 찾아
     * 그냥 current 필드만 true로 변경해준다.
     * 난 조건은 여러 노드에서 martial id가 겹칠 수 있으므로 조건은 파라미터로 받은
     * String processTree에서 기존의 current가 true 되있던 것에 상위에서 뒤져야 한다.
     * **/
    public String returnProcessTreeNextNode(
            String processTreeJson,
            Integer targetMatPk,
            String targetMatName
    ) {

        if (processTreeJson == null || processTreeJson.isBlank()) {
            throw new CustomException("processTree가 비어있습니다.");
        }

        // json -> tree
        Map<String, BomNode> bomTree =
                JsonUtil.parseProcessTree(processTreeJson);

        // 1️⃣ 기존 current 위치 찾기
        CurrentContext ctx = findCurrentNodeContext(bomTree, targetMatPk, targetMatName);
        if (ctx == null) {
            throw new CustomException("현재 진행 중인 공정을 찾지 못했습니다.");
        }

        // 완료한 공정이 현재 current인지 검증
//        if (targetMatPk != null) {
//            if (ctx.current.matPk == null ||
//                    !ctx.current.matPk.equals(targetMatPk)) {
//                throw new CustomException("현재 공정이 아닙니다.");
//            }
//        }

        // 3️⃣ 기존 current 제거
        ctx.root.clearCursor();

        // 4️⃣ 다음 공정 결정
        BomNode next = resolveNextProcess(ctx);

        if (next == null) {
            throw new CustomException("다음 공정을 찾지 못했습니다.");
        }

        // 5️⃣ current 마킹
        next.current = true;

        return JsonUtil.mapToJson(bomTree);
    }

    //기존 current 찾기
    private CurrentContext findCurrentNodeContext(
            Map<String, BomNode> bomTree,
            Integer targetMatPk,
            String targetFlowKey
    ) {

        for (Map.Entry<String, BomNode> entry : bomTree.entrySet()) {

            if(targetFlowKey != null && !targetFlowKey.equals(entry.getKey())){
                continue;
            }

            List<BomNode> path = new ArrayList<>();
            BomNode found = dfsFindCurrent(entry.getValue(), path);

            if (found == null) continue;

            // 🔥 이번에 완료한 작업이 이 트리에 포함된 경우만 선택
            if (targetMatPk != null) {
                boolean match = path.stream()
                        .anyMatch(n -> n.matPk != null && n.matPk.equals(targetMatPk));
                if (!match) continue;
            }

            CurrentContext ctx = new CurrentContext();
            ctx.root = entry.getValue();
            ctx.current = found;
            ctx.path = path;
            return ctx;
        }

        return null;
    }


    private BomNode dfsFindCurrent(BomNode node, List<BomNode> path){

        path.add(node);

        if(node.current){
            return node;
        }

        for(BomNode child : node.children){
            BomNode found = dfsFindCurrent(child, path);
            if(found != null) return found;
        }

        path.remove(path.size() - 1);  // 못 찾았으면 경로에서 제거
        return null;
    }

    private BomNode resolveNextProcess(CurrentContext ctx) {

        List<BomNode> path = ctx.path;
        BomNode current = ctx.current;

        int idx = path.indexOf(current);

        // 1️⃣ 부모가 "실제 공정"이면 → 부모로 이동
        if (idx > 0) {
            BomNode parent = path.get(idx - 1);

            if (!isVirtualRoot(parent) && isProcessNode(parent)) {
                return parent;
            }
        }

        // 2️⃣ 형제 쪽 다음 공정 탐색
        for (int i = idx - 1; i >= 0; i--) {
            BomNode ancestor = path.get(i);
            BomNode parent = (i > 0) ? path.get(i - 1) : null;
            if (parent == null) continue;

            boolean passed = false;
            for (BomNode sibling : parent.children) {
                if (sibling == ancestor) {
                    passed = true;
                    continue;
                }
                if (!passed) continue;

                BomNode leaf = findDeepestProcessLeaf(sibling);
                if (leaf != null && leaf != current) return leaf;
            }
        }

        // 3️⃣ 마지막 → 가상 루트로 이동
        return ctx.root;

    }

    private BomNode findDeepestProcessLeaf(BomNode node) {

        BomNode best = null;

        if (isProcessNode(node)) {
            best = node;
        }

        for (BomNode child : node.children) {
            BomNode found = findDeepestProcessLeaf(child);
            if (found != null) {
                best = found;
            }
        }

        return best;
    }



    private static class CurrentContext {
        BomNode root;
        BomNode current;
        List<BomNode> path;
    }

    private boolean isVirtualRoot(BomNode node){
        return node.matPk == null && node.level == null;
    }

    //////////////////////////////////////////////////////////////////////////////


    public static boolean isProcessNode(BomNode node) {
        return (node.class1 != null && !node.class1.isBlank())
                || (node.class2 != null && !node.class2.isBlank())
                || (node.class3 != null && !node.class3.isBlank());
    }


    public static List<BomNode> findAllCurrent(BomNode node) {
        List<BomNode> result = new ArrayList<>();
        collect(node, result);
        return result;
    }

    private static void collect(BomNode node, List<BomNode> result) {
        if (node.current) {
            result.add(node);
        }
        for (BomNode child : node.children) {
            collect(child, result);
        }
    }
}
