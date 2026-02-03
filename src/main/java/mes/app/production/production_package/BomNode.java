package mes.app.production.production_package;

import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class BomNode {

    public Integer myKey;
    public Integer parentKey;

    public Integer level;

    public String matName;
    public String matCode;
    public String matType;
    public Integer matPk;
    public Integer parentMatPk;
    public String unit;

    public BigDecimal bomRatio;
    public BigDecimal bomQty;
    public BigDecimal calculatedBomRatio;

    public String class1;
    public String class2;
    public String class3;

    public boolean complete;
    public boolean current; //현재 공정
    public Integer storeHouseId;

    public List<BomNode> children = new ArrayList<>();



    public BomNode(Map<String, Object> row) {
        this.myKey = (Integer) row.get("my_key");
        this.parentKey = (Integer) row.get("parent_key");
        this.level = (Integer) row.get("level");
        this.matName = (String) row.get("mat_name");
        this.matCode = (String) row.get("mat_code");
        this.matType = (String) row.get("mat_type");
        this.matPk = (Integer) row.get("mat_pk");
        this.parentMatPk = (Integer) row.get("parent_mat_pk");
        this.unit = (String) row.get("unit");
        this.bomRatio = row.get("bom_ratio") != null
                ? new BigDecimal(row.get("bom_ratio").toString())
                : null;
        this.bomQty = (BigDecimal) row.get("bom_qty");
        this.class1 = row.get("class1") == null ? "" : row.get("class1").toString();
        this.class2 = row.get("class2") == null ? "" : row.get("class2").toString();
        this.class3 = row.get("class3") == null ? "" : row.get("class3").toString();
        this.complete = false;
        this.storeHouseId = (Integer) row.get("storehouse_id");


    }

    /**
     * 특정 품목 공정을 완료 처리하고, 트리의 상태를 다음 단계로 전이시킵니다.
     */
    public void completeProcess(Integer targetMatPk) {
        if (this.matPk != null && this.matPk.equals(targetMatPk)) {
            this.complete = true;
            this.current = false; // 완료되었으므로 더 이상 현재 활성 노드가 아님
        }
        for (BomNode child : this.children) {
            child.completeProcess(targetMatPk);
        }
    }

    /**
     * 공정 취소 시 상태를 되돌립니다.
     */
    public void rollbackProcess(Integer targetMatPk) {
        if (this.matPk != null && this.matPk.equals(targetMatPk)) {
            this.complete = false;
            this.current = true; // 취소된 공정을 다시 '현재 진행 중'으로 변경
        }
        for (BomNode child : this.children) {
            child.rollbackProcess(targetMatPk);
        }
    }

    /**
     * 트리 전체에서 'current' 마킹을 초기화합니다. [2]
     */
    public void clearCursor() {
        this.current = false;
        for (BomNode child : this.children) {
            child.clearCursor();
        }
    }
}
