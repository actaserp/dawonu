package mes.app.production.production_package;

import lombok.Getter;

@Getter
public enum ProcessType {

    SIMPLE_FLOW("3차공정미포함"),
    FULL_FLOW("3차공정포함"); //SINGLE

    private final String description;

    ProcessType(String description){
        this.description = description;
    }

}
