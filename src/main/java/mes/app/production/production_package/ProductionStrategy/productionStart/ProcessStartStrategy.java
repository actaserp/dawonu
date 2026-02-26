package mes.app.production.production_package.ProductionStrategy.productionStart;

import mes.app.production.Enum.ProcessType;
import mes.app.production.production_package.BomNode;
import mes.app.production.production_package.ProcessFlow;
import mes.domain.entity.JobRes;
import mes.domain.entity.User;

import java.util.Map;

public interface ProcessStartStrategy {
    ProcessType getType();
    void start(JobRes jobRes, ProcessFlow flow, Map<ProcessType, BomNode>  ExtraData, User user);

}
