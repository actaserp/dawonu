package mes.app.production.dto;

import mes.app.production.production_package.ProcessFlow;
import mes.app.production.production_package.ProductionStrategy.productionStart.ProcessStartStrategy;
import mes.domain.entity.JobRes;
import mes.domain.entity.MaterialProduce;
import mes.domain.entity.User;
import org.springframework.context.annotation.Description;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Description("직접 생성금지, 무조건 메서드 호출해서 객체 생성하셈 : forStart, forConsumption")
public record BomProcessContext(
        List<Map<String, Object>> bomList,
        ProcessFlow flow,
        JobRes jobRes,
        ProcessStartStrategy strategy,
        User user,
        List<MaterialProduce> mpEntityList,
        Float goodQty,
        String spjangcd
){
    /**
     * 컴팩트 생성자: 방어적 복사를 통해 내부 리스트의 불변성을 보장합니다.
     */
    public BomProcessContext {
        // 리스트가 null일 경우 빈 리스트로 초기화하고, 외부에서 수정 불가능하도록 감쌉니다.
        bomList = (bomList != null) ? List.copyOf(bomList) : Collections.emptyList();
        mpEntityList = (mpEntityList != null) ? List.copyOf(mpEntityList) : Collections.emptyList();
    }

    /**
     * 공정 시작 및 실적 생성 단계에서 사용하는 깨끗한 바구니 생성 팩토리
     */
    public static BomProcessContext forStart(List<Map<String, Object>> bomList, ProcessFlow flow, JobRes jobRes, ProcessStartStrategy strategy, User user) {
        return new BomProcessContext(
                bomList, flow, jobRes, strategy, user, null, null, null
        );
    }

    /**
     * 자재 소모 및 재고 차감 단계 전용 팩토리 (이전 데이터 오염 방지)
     */
    public static BomProcessContext forConsumption(List<Map<String, Object>> bomList, JobRes jobRes, Float goodQty,
                                                   List<MaterialProduce> mpList, User user, String spjangcd) {
        return new BomProcessContext(
                bomList, null, jobRes, null, user, mpList, goodQty, spjangcd
        );
    }

    /**
     * 다음 공정 생성 전용 팩토리 (이전 데이터 오염 방지)
     */
    public static BomProcessContext forCreateNextProcess(JobRes jobRes, User user) {
        return new BomProcessContext(
                null, null, jobRes, null, user, null, null, null
        );
    }

    /**
     * BOM 존재 여부 확인 편의 메서드
     */
    public boolean hasBom() {
        return !bomList.isEmpty();
    }

}
