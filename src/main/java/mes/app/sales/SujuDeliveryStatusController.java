package mes.app.sales;

import lombok.extern.slf4j.Slf4j;
import mes.app.sales.service.SujuDeliveryStatusService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/sales/suju_delivery_status")
public class SujuDeliveryStatusController {

  @Autowired
  SujuDeliveryStatusService sujuDeliveryStatusService;


  @GetMapping("/read")
  public AjaxResult getList(@RequestParam(value = "start")String startStr,
                            @RequestParam(value = "end") String endStr,
                            @RequestParam(value = "company", required = false) String company) {

    LocalDate start = LocalDate.parse(startStr); // "YYYY-MM-DD" 형식 가정
    LocalDate end   = LocalDate.parse(endStr);
    List<Map<String, Object>> item = sujuDeliveryStatusService.getList(start, end, company);

    AjaxResult result = new AjaxResult();
    result.data = item;
    return result;
  }
}
