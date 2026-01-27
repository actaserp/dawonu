package mes.app.production.production_package;

import mes.Exception.CustomException;
import mes.domain.entity.JobRes;
import mes.domain.repository.JobResRepository;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class SimpleProcessStartStrategy extends AbstractProcessStartStrategy{


    public SimpleProcessStartStrategy(JobResRepository repo) {
        super(repo);
    }

    @Override
    protected Map<ProcessType, List<BomNode>> resolveStartNodes(
            ProcessFlow flow,
            Map<ProcessType, BomNode> roots
    ) {
        Map<ProcessType, List<BomNode>> result =
                new EnumMap<>(ProcessType.class);

        BomNode root = roots.get(ProcessType.SIMPLE_FLOW);
        if (root == null) {
            throw new CustomException("SIMPLE_FLOW 루트 없음");
        }

        List<BomNode> currents =
                BomTreeService.findAllCurrent(root);

        if (!currents.isEmpty()) {
            // SIMPLE은 항상 1개
            result.put(ProcessType.SIMPLE_FLOW,
                    List.of(currents.get(0)));
        }

        return result;
    }

    @Override
    public ProcessType getType() {
        return ProcessType.SIMPLE_FLOW;
    }

    @Override
    protected void customize(JobRes child, JobRes parent, ProcessFlow flow, BomNode node){
        //아직은 전략이 다른게 없어서 아무것도 안함....
    }
}
