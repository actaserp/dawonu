package mes.app.production.production_package;

import mes.domain.entity.JobRes;
import mes.domain.entity.User;
import mes.domain.repository.JobResRepository;

import java.math.BigDecimal;
import java.util.Map;

public class SingleProcessStartStrategy implements ProcessStartStrategy{

    private final JobResRepository jobResRepository;

    public SingleProcessStartStrategy(JobResRepository jobResRepository) {
        this.jobResRepository = jobResRepository;
    }

    @Override
    public ProcessType getType() {
        return ProcessType.SINGLE;
    }

    @Override
    public void start(JobRes jobRes, ProcessFlow flow, BomNode node, User user) {

        //set 로직
        JobRes child_job = new JobRes();
        child_job.set_audit(user);
        child_job.setProductionPlanDate(jobRes.getProductionPlanDate());
        child_job.setProductionDate(jobRes.getProductionDate());
        child_job.setShiftCode(jobRes.getShiftCode());
        child_job.setWorkIndex(1);
        child_job.setOrderQty(node.calculatedBomRatio.floatValue());
        child_job.setState("ordered");
        child_job.setParentId(jobRes.getId());
        child_job.setProcessCount(1);
        child_job.setSourceDataPk(jobRes.getSourceDataPk());
        child_job.setSourceTableName(jobRes.getSourceTableName());
        child_job.setFirstWorkCenter_id(jobRes.getFirstWorkCenter_id());

        child_job.setMaterialId(node.matPk);

        child_job.setStoreHouse_id(node.storeHouseId);
        child_job.setWorkCenter_id(jobRes.getWorkCenter_id());
        child_job.setSpjangcd(jobRes.getSpjangcd());

        jobResRepository.save(child_job);
    }
}
