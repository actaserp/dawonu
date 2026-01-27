package mes.app.production.dto.productionResult;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class WorkFinishRequest {

    private Integer id;
    private String lot_num;
    private Float order_qty;
    private Float good_qty;
    private Float defect_qty;
    private Float loss_qty;
    private Float scrap_qty;

    private String shift_code;
    private Integer mat_pk;
    private Integer workcenter_id;
    private Integer equipment_id;

    private String prod_date;
    private String start_time;
    private String end_date;
    private String end_time;

    private String description;
    private String order_num;

    private String invatyn;   // Y / N
    private String spjangcd;  // sessionStorage 값
}
