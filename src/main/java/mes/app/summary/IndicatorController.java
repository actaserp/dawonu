package mes.app.summary;

import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.DayOfWeek;
import java.time.YearMonth;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/summary/indicator")
public class IndicatorController {

	@Autowired
	SqlRunner sqlRunner;

	@GetMapping("/prod_read")
	public AjaxResult getProductionMonthList(
			@RequestParam(value = "cboYear", required = false) String cboYear,
			@RequestParam(value = "spjangcd") String spjangcd) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_form", cboYear + "-01-01");
		paramMap.addValue("date_to", cboYear + "-12-31");
		paramMap.addValue("spjangcd", spjangcd);

		//현재 매출액(출고수량 * 단가) / (((월초 재고 + 월말 재고) / 2) * 제품 평균단가) 이렇게 되있음.
		//근데 매출액이 아니라 원래는 매출원가로 해주고 평균재고액 또한 원가 기준으로 해야함
		//완료보고서에서 매출액이라고 되어있어서 이렇게 함. 그리고 현재 BOM 등록이 제대로 안되있음.
		//나중에는 수량 기준으로 하는게 더 정확한 재고회전율이 나올 수 있음.

		StringBuilder sql = new StringBuilder("""
				WITH A AS (
				    -- 출하 합계 (매출원가)
				    SELECT
				        m."id" AS mat_pk,
				        EXTRACT(MONTH FROM sh."ShipDate") AS data_month,
				        SUM(shm."Price") AS money_sum
				    FROM material m
				    LEFT JOIN shipment shm ON shm."Material_id" = m.id
				    LEFT JOIN shipment_head sh ON sh.id = shm."ShipmentHead_id"
				        AND sh."ShipDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
				        AND sh."State" = 'shipped'
				    WHERE m.spjangcd = :spjangcd
				    GROUP BY m."id", EXTRACT(MONTH FROM sh."ShipDate")
				),
                """);

		sql.append("""
				stock_avg AS (
				     SELECT
				         m.id AS mat_pk,
				         EXTRACT(MONTH FROM gs.month_start) AS data_month,
				         ROUND(
				             (
				                 m."UnitPrice" *
				                 (
				                     (
				                         -- 월초 재고
				                         COALESCE((
				                             SELECT SUM("InputQty") - SUM("OutputQty")
				                             FROM mat_inout io
				                             WHERE io."Material_id" = m.id
				                               AND io."InoutDate" < gs.month_start
				                         ), 0)
				                         +
				                         -- 월말 재고
				                         COALESCE((
				                             SELECT SUM("InputQty") - SUM("OutputQty")
				                             FROM mat_inout io
				                             WHERE io."Material_id" = m.id
				                               AND io."InoutDate" <= (gs.month_start + interval '1 month - 1 day')
				                         ), 0)
				                     ) / 2.0
				                 )
				             )::numeric
				         , 2) AS avg_inv_value
				     FROM material m
				     CROSS JOIN (
				         SELECT generate_series(
				             DATE_TRUNC('year', CAST(:date_form AS DATE)),
				             DATE_TRUNC('year', CAST(:date_form AS DATE)) + interval '11 month',
				             interval '1 month'
				         ) AS month_start
				     ) gs
				 )
				""");

		sql.append("""
				SELECT
				1 AS grp_idx,
				mg."Name" AS mat_grp_name,
				m."Code" AS mat_code,
				m."Name" AS mat_name,
				A.mat_pk,
				u."Name" AS unit_name,
				m."UnitPrice" AS unit_price,
				SUM(A.money_sum) AS year_money_sum
                """);

		for (int i = 1; i <= 12; i++) {
			sql.append(String.format(
					", MIN(CASE WHEN A.data_month = %d THEN A.money_sum END) AS mon_%d_money", i, i
			));
			sql.append(String.format(
					", MIN(CASE WHEN s.data_month = %d THEN s.avg_inv_value END) AS mon_%d_avginv", i, i
			));
			sql.append(String.format(
					", ROUND( (MIN(CASE WHEN A.data_month = %d THEN A.money_sum END) / NULLIF(MIN(CASE WHEN s.data_month = %d THEN s.avg_inv_value END), 0))::numeric, 2) AS mon_%d_turnover",
					i, i, i
			));

		}
		sql.append("""
			FROM A
			INNER JOIN material m ON m.id = A.mat_pk
			LEFT JOIN unit u ON u.id = m."Unit_id"
			LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
			LEFT JOIN stock_avg s ON s.mat_pk = A.mat_pk AND s.data_month = A.data_month
			GROUP BY mg."Name", m."Code", m."Name", A.mat_pk, u."Name", m."UnitPrice"
			ORDER BY A.mat_pk
		""");

		List<Map<String, Object>> rows = sqlRunner.getRows(sql.toString().toString(), paramMap);

		AjaxResult result = new AjaxResult();
		result.data = rows;
		return result;
	}

	private Map<Integer, Integer> getWorkdaysPerMonth(int year) {
		Map<Integer, Integer> workdays = new LinkedHashMap<>();
		for (int month = 1; month <= 12; month++) {
			YearMonth ym = YearMonth.of(year, month);
			int count = 0;
			for (int d = 1; d <= ym.lengthOfMonth(); d++) {
				DayOfWeek dow = ym.atDay(d).getDayOfWeek();
				if (dow != DayOfWeek.SATURDAY && dow != DayOfWeek.SUNDAY) {
					count++;
				}
			}
			workdays.put(month, count);
		}
		return workdays;
	}

	@GetMapping("/short_read")
	public AjaxResult getShortMonthList(
			@RequestParam(value = "cboYear", required = false) String cboYear,
			@RequestParam(value = "spjangcd") String spjangcd) {

		int year = Integer.parseInt(cboYear);
		Map<Integer, Integer> workdays = getWorkdaysPerMonth(year); // 월별 영업일수 계산

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_form", year + "-01-01");
		paramMap.addValue("date_to", year + "-12-31");
		paramMap.addValue("spjangcd", spjangcd);

		StringBuilder sql = new StringBuilder();
		sql.append("""
			WITH base_data AS (
				SELECT 
					s.id AS suju_id,
					s."Material_id" AS mat_pk,
					EXTRACT(MONTH FROM s."JumunDate") AS mon,
					(sh."ShipDate" - s."JumunDate") +1 AS wday,
					s."SujuQty",
					SUM(sp."Qty") OVER (PARTITION BY s.id) AS total_qty
					,s."Company_id" as company_id
				FROM suju s
				JOIN shipment sp ON s.id = sp."SourceDataPk"
				JOIN shipment_head sh ON sp."ShipmentHead_id" = sh.id
				WHERE s."JumunDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
				  AND sh."State" = 'shipped'
				  AND s."spjangcd" = :spjangcd
			),
			mat_data AS (
				SELECT 
					m.id AS mat_pk,
					fn_code_name('mat_type', mg."MaterialType") AS mat_type_name,
					mg."Name" AS mat_grp_name,
					m."Code" AS mat_code,
					m."Name" AS mat_name,
					u."Name" AS unit_name,
					m."Standard1" AS standard1
				FROM material m
				LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
				LEFT JOIN unit u ON u.id = m."Unit_id"
			)
			SELECT 
				b.mat_pk,
				m.mat_type_name,
				m.mat_code,
				m.mat_name,
				m.unit_name,
				m.standard1,
				c."Name" as company_name, 
		""");

		for (int i = 1; i <= 12; i++) {
			sql.append("ROUND(AVG(CASE WHEN b.mon = ").append(i).append(" THEN b.wday ELSE NULL END)::numeric, 1) AS mon_").append(i);
			if (i < 12) sql.append(", ");
		}

		// ✅ 연평균 컬럼 추가
		sql.append(", ROUND( (");

		for (int i = 1; i <= 12; i++) {
			sql.append("COALESCE(AVG(CASE WHEN b.mon = ").append(i)
					.append(" THEN b.wday END), 0)");
			if (i < 12) sql.append(" + ");
		}

		sql.append(") / NULLIF((");

		for (int i = 1; i <= 12; i++) {
			sql.append("(CASE WHEN COUNT(CASE WHEN b.mon = ").append(i)
					.append(" THEN 1 END) > 0 THEN 1 ELSE 0 END)");
			if (i < 12) sql.append(" + ");
		}

		sql.append("), 0), 1) AS average_year");

		sql.append("""
			FROM base_data b
			JOIN mat_data m ON b.mat_pk = m.mat_pk
			JOIN company c ON b.company_id = c.id
			WHERE b."SujuQty" <= b.total_qty
			GROUP BY b.mat_pk, b.company_id, m.mat_type_name, m.mat_code, m.mat_name, m.unit_name, m.standard1, c."Name"
			ORDER BY m.mat_name
		""");


		List<Map<String, Object>> rows = sqlRunner.getRows(sql.toString(), paramMap);

		AjaxResult result = new AjaxResult();
		result.data = rows;
		return result;
	}

}
