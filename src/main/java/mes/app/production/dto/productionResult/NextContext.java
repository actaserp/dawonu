package mes.app.production.dto.productionResult;

import mes.app.production.production_package.BomNode;

public class NextContext {

    private BomNode nextNode;
    private String flowKey;

    public BomNode getNextNode() {
        return nextNode;
    }

    public void setNextNode(BomNode nextNode) {
        this.nextNode = nextNode;
    }

    public String getFlowKey() {
        return flowKey;
    }

    public void setFlowKey(String flowKey) {
        this.flowKey = flowKey;
    }
}
