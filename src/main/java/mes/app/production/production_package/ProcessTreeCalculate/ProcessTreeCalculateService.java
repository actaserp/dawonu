package mes.app.production.production_package.ProcessTreeCalculate;

import mes.Exception.CustomException;
import mes.app.production.dto.productionResult.NextContext;
import mes.app.production.production_package.BomNode;
import mes.app.production.production_package.BomTreeService;
import mes.app.util.JsonUtil;
import mes.domain.entity.JobRes;
import mes.domain.entity.JobResProcessTree;
import mes.domain.entity.User;
import mes.domain.repository.JobResProcessTreeRepository;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ProcessTreeCalculateService {

    private final JobResProcessTreeRepository treeRepo;
    private final BomTreeService bomTreeService;


    public ProcessTreeCalculateService(JobResProcessTreeRepository treeRepository, BomTreeService bomTreeService) {
        this.treeRepo = treeRepository;
        this.bomTreeService = bomTreeService;
    }

    /**
     * 완료취소 / 삭제 공통 처리
     * 👉 "해당 FLOW 내부에서만" current 복구
     */
    /// //////////////////////////////////////////////         삭제 및 완료취소시 프로세스 트리 롤백 //////////////////////////////////////////////////////////////////////////
    public void rollbackToSelfProcess(JobRes jr) {

        JobResProcessTree jpt =
                treeRepo.findByWorkOrderNo(jr.getWorkOrderNumber());

        if (jpt == null) {
            throw new CustomException("공정 트리를 찾을 수 없습니다.");
        }

        Map<String, BomNode> tree =
                JsonUtil.parseProcessTree(jpt.getProcessTree());

        // 1️⃣ 대상 FLOW 판단
        String flowKey = resolveFlowKey(tree, jr.getMaterialId());

        BomNode root = tree.get(flowKey);
        if (root == null) {
            throw new CustomException("FLOW 루트를 찾을 수 없습니다: " + flowKey);
        }

        // 2️⃣ 해당 FLOW만 current 초기화
        root.clearCursor();

        // 3️⃣ 해당 FLOW 안에서만 current 복구
        boolean restored =
                restoreCurrentByMaterial(root, jr.getMaterialId());

        if (!restored) {
            throw new CustomException("취소 대상 공정을 FLOW 트리에서 찾지 못했습니다.");
        }

        // 4️⃣ DB 반영
        treeRepo.updateProcessTreeOnly(
                jr.getWorkOrderNumber(),
                JsonUtil.mapToJson(tree)
        );
    }

    /**
     * materialId 기준으로 어느 FLOW 인지 판별
     */
    private String resolveFlowKey(
            Map<String, BomNode> tree,
            Integer matPk
    ) {
        for (Map.Entry<String, BomNode> entry : tree.entrySet()) {
            if (containsMaterial(entry.getValue(), matPk)) {
                return entry.getKey();
            }
        }
        throw new CustomException("공정이 어떤 FLOW에도 속하지 않습니다.");
    }

    private boolean containsMaterial(BomNode node, Integer matPk) {
        if (node.matPk != null && node.matPk.equals(matPk)) {
            return true;
        }
        for (BomNode child : node.children) {
            if (containsMaterial(child, matPk)) {
                return true;
            }
        }
        return false;
    }

    private boolean restoreCurrentByMaterial(
            BomNode node,
            Integer matPk
    ) {
        if (node.matPk != null && node.matPk.equals(matPk)) {
            node.current = true;
            node.complete = false;
            return true;
        }

        for (BomNode child : node.children) {
            if (restoreCurrentByMaterial(child, matPk)) {
                return true;
            }
        }
        return false;
    }

    /// ///////////////////////////////////////////   롤백로직 끝      //////////////////////////// ////////////////////////////////////////


    /// ///////////////////////////////////////////   추가로직 (다음 공정 current=true)  //////////////////////////// ////////////////////////////////////////
    public NextContext createNextProcess(JobRes jr, User user) {

        JobResProcessTree jpt = treeRepo.findByWorkOrderNo(jr.getWorkOrderNumber());
        if(jpt == null) throw new CustomException("해당 작업지시에 대한 공정을 찾을 수 없습니다.");

        String processTree = jpt.getProcessTree();

        Integer matId = jr.getMaterialId();
        Map<String, BomNode> bomTree = JsonUtil.parseProcessTree(processTree);

        //다음 공정 노드 찾기
        NextContext nextCtx = bomTreeService.completeAndGetParentNode(bomTree, matId);
        //담 공정 없으면 스킵
        if(nextCtx == null) return null;

        // 다음 공정트리 구하기
        String nextTree =
                new BomTreeService()
                        .returnProcessTreeNextNode(processTree, nextCtx.getNextNode().matPk, nextCtx.getFlowKey());

        // job_res_tree도 최신화
        // process Tree 도 최신화
        int cnt = treeRepo.updateProcessTreeOnly(jr.getWorkOrderNumber(), nextTree);
        if(cnt <= 0) throw new CustomException("해당 작업지시에 대한 공정을 찾을 수 없습니다.");

        return nextCtx;
    }
}
