package mes.app.production.production_package.ProductionStrategy;

import mes.app.production.Enum.ProcessType;
import mes.app.production.production_package.BomNode;
import mes.app.production.production_package.BomTreeService;
import mes.app.production.production_package.ProcessFlow;
import mes.domain.entity.JobRes;
import mes.domain.repository.JobResRepository;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class FullFlowStartStrategy extends AbstractProcessStartStrategy {

    public FullFlowStartStrategy(JobResRepository repo) {
        super(repo);
    }

    @Override
    protected Map<ProcessType, List<BomNode>> resolveStartNodes(
            ProcessFlow flow,
            Map<ProcessType, BomNode> roots
    ) {
        Map<ProcessType, List<BomNode>> result =
                new EnumMap<>(ProcessType.class);

        // =========================
        // 1️⃣ SIMPLE_FLOW (항상)
        // =========================
        BomNode simpleRoot = roots.get(ProcessType.SIMPLE_FLOW);
        if (simpleRoot != null) {
            List<BomNode> simpleCurrents =
                    BomTreeService.findAllCurrent(simpleRoot);

            if (!simpleCurrents.isEmpty()) {
                result.put(ProcessType.SIMPLE_FLOW, simpleCurrents);
            }
        }

        // =========================
        // 2️⃣ FULL_FLOW (optional)
        // =========================
        if (flow.hasThirdProcess()) {

            BomNode fullRoot = roots.get(ProcessType.FULL_FLOW);
            if (fullRoot != null) {

                List<BomNode> fullCurrents =
                        BomTreeService.findAllCurrent(fullRoot);

                if (!fullCurrents.isEmpty()) {
                    result.put(ProcessType.FULL_FLOW, fullCurrents);
                }
            }
        }

        return result;
    }

    @Override
    public ProcessType getType() {
        return ProcessType.FULL_FLOW;
    }

    @Override
    protected void customize(JobRes child, JobRes parent, ProcessFlow flow, BomNode node){
        //아직은 전략이 다른게 없어서 아무것도 안함....
    }


}
