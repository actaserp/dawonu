package mes.app.production.production_package;

import mes.Exception.CustomException;
import mes.app.util.UtilClass;
import mes.domain.entity.JobRes;
import mes.domain.entity.Material;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ProcessFlow {
    private final List<String> processes;
    private final boolean hasFirstProcess;

    private ProcessFlow(List<String> processes, boolean hasFirstProcess) {
        this.processes = processes;
        this.hasFirstProcess = hasFirstProcess;
    }

    //판단에 대한 책임은 from 메서드가 해야한다. 그래서 생성자는 private으로 캡슐화
    public static ProcessFlow from(Material m){

        boolean hasFirst = m.getClass1() != null && !m.getClass1().isBlank();

        List<String> list = Stream.of(
                m.getClass1(), m.getClass2(), m.getClass3()
        ).filter(s -> s != null && !s.isBlank())
        .toList();

        if(list.isEmpty()) throw new CustomException("해당 제품에 대한 등록된 공정이 없습니다.");

        return new ProcessFlow(list, hasFirst);
    }

    //1차 공정을 포함하는지 안하는지(3차 단독공정) 여부
    public ProcessType startType(){
        return hasFirstProcess
                ? ProcessType.FIRST_CONTAINS : ProcessType.SINGLE;
    }


    public boolean hasFirstProcess(){
        return hasFirstProcess;
    }

    public String first(){
        return processes.get(0);
    }

    public List<String> all(){
        return processes;
    }

    public int cnt(){
        return processes.size();
    }


}
