package mes.app.production.production_package;

import mes.domain.entity.JobRes;
import mes.domain.entity.User;

import java.util.Map;

public interface ProcessStartStrategy {
    ProcessType getType();
    void start(JobRes jobRes, ProcessFlow flow, Map<ProcessType, BomNode>  ExtraData, User user);

}
