package mes.app.production.production_package.ProductionStrategy;


import mes.Exception.CustomException;
import mes.app.production.Enum.ProcessType;
import mes.app.production.production_package.BomNode;
import mes.app.production.production_package.ProcessFlow;
import mes.app.util.UtilClass;
import mes.domain.entity.JobRes;
import mes.domain.entity.User;
import mes.domain.repository.JobResRepository;

import java.util.List;
import java.util.Map;

public abstract class AbstractProcessStartStrategy implements ProcessStartStrategy {

    protected final JobResRepository jobResRepository;

    protected AbstractProcessStartStrategy(JobResRepository repo){
        this.jobResRepository = repo;
    }

    @Override
    public final void start(
            JobRes parent,
            ProcessFlow flow,
            Map<ProcessType, BomNode> roots,
            User user
    ) {

        // 🔥 변경된 부분: List<BomNode>
        Map<ProcessType, List<BomNode>> startNodes =
                resolveStartNodes(flow, roots);

        if (startNodes.isEmpty()) {
            throw new CustomException("시작 공정 노드를 찾을 수 없습니다.");
        }

        int workIndex = 1;

        for (Map.Entry<ProcessType, List<BomNode>> entry : startNodes.entrySet()) {

            ProcessType type = entry.getKey();
            List<BomNode> currents = entry.getValue();

            if (currents == null || currents.isEmpty()) continue;

            // 🔹 같은 ProcessType(FULL_FLOW 병렬)는 같은 workIndex
            for (BomNode current : currents) {

                JobRes child = new JobRes();

                // ======================
                // ✅ 공통 로직
                // ======================
                child.set_audit(user);
                child.setProductionPlanDate(parent.getProductionPlanDate());
                child.setProductionDate(parent.getProductionDate());
                child.setShiftCode(parent.getShiftCode());
                child.setWorkIndex(workIndex);
                child.setProcessCount(1); // 시작 공정
                child.setOrderQty(
                        UtilClass.DecimalToFloat(current.calculatedBomRatio)
                );
                child.setState("ordered");
                child.setParentId(parent.getId());
                child.setSourceDataPk(parent.getSourceDataPk());
                child.setSourceTableName(parent.getSourceTableName());
                child.setFirstWorkCenter_id(parent.getFirstWorkCenter_id());
                child.setMaterialId(current.matPk);
                child.setStoreHouse_id(current.storeHouseId);
                child.setWorkCenter_id(parent.getWorkCenter_id());
                child.setSpjangcd(parent.getSpjangcd());

                // ======================
                // 🔽 전략별 차이
                // ======================
                customize(child, parent, flow, current);

                jobResRepository.save(child);
            }

            // 👉 다음 공정 단계로 이동
            workIndex++;
        }
    }

    protected abstract Map<ProcessType, List<BomNode>> resolveStartNodes(
            ProcessFlow flow,
            Map<ProcessType, BomNode> roots
    );

    protected abstract void customize(
            JobRes child,
            JobRes parent,
            ProcessFlow flow,
            BomNode node
    );

}
