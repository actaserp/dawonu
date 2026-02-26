package mes.app.production.production_package.ProductionStrategy.Bomprocess;

import lombok.RequiredArgsConstructor;
import mes.app.production.dto.BomProcessContext;
import mes.domain.entity.JobRes;
import mes.domain.repository.JobResRepository;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManager;

@Component
@RequiredArgsConstructor
public class EmptyBomProcessor implements BomProcessor{

    private final JobResRepository jobResRepository;
    private final EntityManager entityManager;

    @Override
    public void createJobRes(BomProcessContext context) {
        JobRes header = context.jobRes();

        header = jobResRepository.save(header);
        jobResRepository.flush();
        entityManager.refresh(header);
    }

    @Override
    public void consumeMaterials(BomProcessContext context) {
        //투입재고 차감안함
    }

    @Override
    public void createNextProcess(BomProcessContext context) {
        //다음 공정 생산안함
    }
}
