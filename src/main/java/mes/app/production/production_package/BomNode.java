package mes.app.production.production_package;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        this.storeHouseId = (Integer) row.get("storehouse_id");
    }

}
