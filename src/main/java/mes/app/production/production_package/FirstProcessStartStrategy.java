package mes.app.production.production_package;

import mes.domain.entity.JobRes;
import mes.domain.entity.User;
import mes.domain.repository.JobResRepository;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

@Component
public class FirstProcessStartStrategy implements ProcessStartStrategy{

    private final JobResRepository jobResRepository;

    public FirstProcessStartStrategy(JobResRepository jobResRepository) {
        this.jobResRepository = jobResRepository;
    }

    @Override
    public ProcessType getType() {
        return ProcessType.FIRST_CONTAINS;
    }

    @Override
    public void start(JobRes jobRes, ProcessFlow flow, BomNode node, User user) {



        //1차 공정 생성
        JobRes child_job = new JobRes();
        child_job.set_audit(user);
        child_job.setProductionPlanDate(jobRes.getProductionPlanDate());
        child_job.setProductionDate(jobRes.getProductionDate());
        child_job.setShiftCode(jobRes.getShiftCode());
        child_job.setWorkIndex(1);
        child_job.setOrderQty(node.calculatedBomRatio.floatValue()); //TODO
        child_job.setState("ordered");
        child_job.setParentId(jobRes.getId());
        child_job.setProcessCount(1); //첫 공정이니 1
        child_job.setSourceDataPk(jobRes.getSourceDataPk());
        child_job.setSourceTableName(jobRes.getSourceTableName());
        child_job.setFirstWorkCenter_id(jobRes.getFirstWorkCenter_id());

        child_job.setMaterialId(node.matPk); //TODO

        child_job.setStoreHouse_id(node.storeHouseId); //TODO
        child_job.setWorkCenter_id(jobRes.getWorkCenter_id());
        child_job.setSpjangcd(jobRes.getSpjangcd());
        // ... 이후로 계속 엔티티에 set하는 작업

        jobResRepository.save(child_job);
    }
}
