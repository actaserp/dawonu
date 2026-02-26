package mes.app.production.production_package;

import mes.Exception.CustomException;
import mes.app.production.Enum.ProcessType;
import mes.domain.entity.Material;

import java.util.List;
import java.util.stream.Stream;
/**
 * 다원유는 1->2차는 관련이 있음,  3차는 관련이 없음
 * 3차는 다른 원재료로 생산하고 2차와 3차 결합해서 완제품 되는 병렬 브랜치 구조.
 * ***/
public class ProcessFlow {
    private final List<String> processes;
    private final boolean hasClass1;
    private final boolean hasClass2;
    private final boolean hasClass3;

    private ProcessFlow(List<String> processes, boolean hasClass1, boolean hasClass2, boolean hasClass3) {
        this.processes = processes;
        this.hasClass1 = hasClass1;
        this.hasClass2 = hasClass2;
        this.hasClass3 = hasClass3;
    }

    //판단에 대한 책임은 from 메서드가 해야한다. 그래서 생성자는 private으로 캡슐화
    public static ProcessFlow from(Material m) {

        boolean c1 = m.getClass1() != null && !m.getClass1().isBlank();
        boolean c2 = m.getClass2() != null && !m.getClass2().isBlank();
        boolean c3 = m.getClass3() != null && !m.getClass3().isBlank();

        List<String> list = Stream.of(
                        m.getClass1(), m.getClass2(), m.getClass3()
                ).filter(s -> s != null && !s.isBlank())
                .toList();

//        if (list.isEmpty()) {
//            throw new CustomException("해당 제품에 대한 등록된 공정이 없습니다.");
//        }


        return new ProcessFlow(list, c1, c2, c3);
    }

    /* ================= 판단 로직 ================= */

    /** 3차 공정 포함 여부 (병렬 브랜치 존재) **/
    public boolean hasThirdProcess(){
        return hasClass3;
    }
    /** 1·2차 직렬 라인 존재 여부 */
    public boolean hasMainLine() {
        return hasClass1 || hasClass2;
    }

    public boolean hasProcesses() {
        // 리스트가 null이 아니고, 비어있지도 않은지 확인
        return processes != null && !processes.isEmpty();
    }

    /** 전략 선택용 */
    public ProcessType startType() {

        if(!hasProcesses()){
            return ProcessType.SIMPLE_FLOW;
        }

        if(hasThirdProcess()){
            return ProcessType.FULL_FLOW;
        }
        return ProcessType.SIMPLE_FLOW;
    }

    /** 공정 개수 */
    public int cnt() {
        return processes.size();
    }

    ///전략에 따라 workIndex
    public int getNextWorkIndex() {
        return switch (startType()) {
            case FULL_FLOW   -> cnt() + 1;
            case SIMPLE_FLOW -> cnt();
        };
    }


}
