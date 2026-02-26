package mes.app.production.production_package.ProductionStrategy.Bomprocess;

import mes.app.production.dto.BomProcessContext;
import mes.app.production.production_package.BomNode;

import java.util.Map;

public interface BomProcessor {

    //작업지시 생성
    void createJobRes(BomProcessContext context);

    //재고차감
    void consumeMaterials(BomProcessContext context);

    void createNextProcess(BomProcessContext context);
}
