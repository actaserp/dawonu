package mes.app.production.production_package;

import lombok.Getter;

@Getter
public enum ProcessType {

    FIRST_CONTAINS("1차공정포함"),
    SINGLE("3차단독공정");

    private final String description;

    ProcessType(String description){
        this.description = description;
    }

}
