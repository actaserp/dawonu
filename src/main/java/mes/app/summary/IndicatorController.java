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

		//현재 매출액(출고수량 * 단가) / (((월초 재고 + 월말 재고) / 2) * 월별 평균 출하단가) 이렇게 되있음.
		//근데 매출액이 아니라 원래는 매출원가로 해주고 평균재고액 또한 원가 기준으로 해야함 --> 매출원가가 재료비는 아님 매출원가는 재료비+간접비+노무비 합산인데 이걸 어케 구하냐는것
		//완료보고서에서 매출액이라고 되어있어서 이렇게 함. 그리고 현재 BOM 등록이 제대로 안되있음.
		//나중에는 수량 기준으로 하는게 더 정확한 재고회전율이 나올 수 있음.

		//추가로 재료비 (BOM) 기반으로 한다? --> 서브쿼리 및 다수의 조인 발생해서 성능병목 생긴다. 차라리 수량 기반의 재고회전율을 보는게 나을듯함.
		StringBuilder sql = new StringBuilder("""
WITH A AS (
    -- 월별 매출액: 규격(숫자면 곱함) × 수량 × (부가세 반영 단가)
    SELECT
        m.id AS mat_pk,
        EXTRACT(MONTH FROM sh."ShipDate") AS data_month,
        SUM(
            (CASE
                WHEN suju."Standard" ~ '^[0-9]+(\\.[0-9]+)?$' THEN suju."Standard"::numeric
                ELSE 1
            END)
            * COALESCE(shm."Qty", 0)
            * (CASE WHEN suju."InVatYN" = 'Y' THEN shm."UnitPrice" / 1.1 ELSE shm."UnitPrice" END)
        )::numeric AS money_sum
    FROM material m
    LEFT JOIN shipment shm ON shm."Material_id" = m.id
    LEFT JOIN shipment_head sh ON sh.id = shm."ShipmentHead_id"
        AND sh."ShipDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
        AND sh."State" = 'shipped'
    LEFT JOIN suju ON shm."SourceTableName" = 'rela_data'
        AND shm."SourceDataPk" = suju.id
    WHERE m.spjangcd = :spjangcd
    GROUP BY m.id, EXTRACT(MONTH FROM sh."ShipDate")
),
avg_price AS (
    -- 월별 평균 출하단가 (규격 제외, InVatYN 반영)
    SELECT
        m.id AS mat_pk,
        EXTRACT(MONTH FROM sh."ShipDate") AS data_month,
        ROUND((
            SUM(
                COALESCE(shm."Qty", 0)
                * (CASE WHEN suju."InVatYN" = 'Y' THEN shm."UnitPrice" / 1.1 ELSE shm."UnitPrice" END)
            )
            / NULLIF(SUM(COALESCE(shm."Qty", 0)), 0)
        )::numeric, 2) AS avg_ship_price
    FROM material m
    LEFT JOIN shipment shm ON shm."Material_id" = m.id
    LEFT JOIN shipment_head sh ON sh.id = shm."ShipmentHead_id"
        AND sh."ShipDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
        AND sh."State" = 'shipped'
    LEFT JOIN suju ON shm."SourceTableName" = 'rela_data'
        AND shm."SourceDataPk" = suju.id
    WHERE m.spjangcd = :spjangcd
    GROUP BY m.id, EXTRACT(MONTH FROM sh."ShipDate")
),
stock_avg AS (
    SELECT
        m.id AS mat_pk,
        EXTRACT(MONTH FROM gs.month_start) AS data_month,
        ROUND((
            (
                COALESCE((
                    SELECT SUM("InputQty") - SUM("OutputQty")
                    FROM mat_inout io
                    WHERE io."Material_id" = m.id
                      AND io."InoutDate" <= gs.month_start
                ), 0)
                +
                COALESCE((
                    SELECT SUM("InputQty") - SUM("OutputQty")
                    FROM mat_inout io
                    WHERE io."Material_id" = m.id
                      AND io."InoutDate" <= (gs.month_start + interval '1 month - 1 day')
                ), 0)
            ) / 2::numeric
            * COALESCE(ap.avg_ship_price, m."UnitPrice"::numeric)
        )::numeric, 2) AS avg_inv_value
    FROM material m
    CROSS JOIN (
        SELECT generate_series(
            DATE_TRUNC('year', CAST(:date_form AS DATE)),
            DATE_TRUNC('year', CAST(:date_form AS DATE)) + interval '11 month',
            interval '1 month'
        ) AS month_start
    ) gs
    LEFT JOIN avg_price ap
           ON ap.mat_pk = m.id
          AND ap.data_month = EXTRACT(MONTH FROM gs.month_start)
),
year_summary AS (
    -- 연간 요약: 연초/연말 재고수량, 연간 평균단가(규격 제외), 연간 총매출액(규격 포함)
    SELECT
        m.id AS mat_pk,
        COALESCE((
            SELECT SUM("InputQty") - SUM("OutputQty")
            FROM mat_inout io
            WHERE io."Material_id" = m.id
              AND io."InoutDate" <= DATE_TRUNC('year', CAST(:date_form AS DATE))
        ), 0) AS start_stock,
        COALESCE((
            SELECT SUM("InputQty") - SUM("OutputQty")
            FROM mat_inout io
            WHERE io."Material_id" = m.id
              AND io."InoutDate" <= (DATE_TRUNC('year', CAST(:date_to AS DATE)) + interval '1 year - 1 day')
        ), 0) AS end_stock,
        -- 연간 평균단가(규격 제외)
        (
            SUM(
                COALESCE(shm."Qty", 0)
                * (CASE WHEN suju."InVatYN" = 'Y' THEN shm."UnitPrice" / 1.1 ELSE shm."UnitPrice" END)
            )
            / NULLIF(SUM(COALESCE(shm."Qty", 0)), 0)
        )::numeric AS avg_price_year,
        -- 연간 총매출액(규격 포함)
        SUM(
            (CASE
                WHEN suju."Standard" ~ '^[0-9]+(\\.[0-9]+)?$' THEN suju."Standard"::numeric
                ELSE 1
            END)
            * COALESCE(shm."Qty", 0)
            * (CASE WHEN suju."InVatYN" = 'Y' THEN shm."UnitPrice" / 1.1 ELSE shm."UnitPrice" END)
        )::numeric AS total_sales_year
    FROM material m
    LEFT JOIN shipment shm ON shm."Material_id" = m.id
    LEFT JOIN shipment_head sh ON sh.id = shm."ShipmentHead_id"
        AND sh."ShipDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
        AND sh."State" = 'shipped'
    LEFT JOIN suju ON shm."SourceTableName" = 'rela_data'
        AND shm."SourceDataPk" = suju.id
    WHERE m.spjangcd = :spjangcd
    GROUP BY m.id
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
    SUM(A.money_sum)::numeric AS year_money_sum
""");

		for (int i = 1; i <= 12; i++) {
			sql.append(String.format(
					", MIN(CASE WHEN A.data_month = %d THEN A.money_sum END)::numeric AS mon_%d_money", i, i
			));
			sql.append(String.format(
					", MIN(CASE WHEN s.data_month = %d THEN s.avg_inv_value END)::numeric AS mon_%d_avginv", i, i
			));
			sql.append(String.format(
					", ROUND( (" +
							"    (MIN(CASE WHEN A.data_month = %1$d THEN A.money_sum END))::numeric" +
							"    / NULLIF((MIN(CASE WHEN s.data_month = %1$d THEN s.avg_inv_value END))::numeric, 0)" +
							"  ), 2) AS mon_%1$d_turnover",
					i, i, i
			));
		}

		sql.append("""
  , ROUND(
  (
    ys.total_sales_year::numeric
    /
    NULLIF(
      (
        ((ys.start_stock + ys.end_stock) / 2::numeric)
        * COALESCE(ys.avg_price_year, m."UnitPrice"::numeric)
      )::numeric
    , 0)
  )::numeric
, 2) AS year_turnover
				FROM material m
				                                                 LEFT JOIN A ON A.mat_pk = m.id
				                                                 LEFT JOIN unit u ON u.id = m."Unit_id"
				                                                 LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
				                                                 LEFT JOIN stock_avg s ON s.mat_pk = m.id AND s.data_month = A.data_month
				                                                 LEFT JOIN year_summary ys ON ys.mat_pk = m.id
GROUP BY mg."Name", m."Code", m."Name", A.mat_pk, u."Name", m."UnitPrice", ys.total_sales_year, ys.start_stock, ys.end_stock, ys.avg_price_year
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
			@RequestParam(value = "date_from") String date_from,
			@RequestParam(value = "date_to") String date_to,
			@RequestParam(value = "spjangcd") String spjangcd
			) {


		StringBuilder sql = new StringBuilder();

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_from", date_from);
		paramMap.addValue("date_to", date_to);
		paramMap.addValue("spjangcd", spjangcd);


		sql.append("""
			WITH suju_sum AS (
				SELECT
					s.id AS suju_id,
					s."JumunNumber" AS jumun_number,
					s."JumunDate" AS jumundate,
					sh."ShipDate" AS shipdate,
					EXTRACT(MONTH FROM s."JumunDate") AS mon,
					EXTRACT(YEAR FROM s."JumunDate") AS yyyy,
					(sh."ShipDate" - s."JumunDate") + 1 AS wday,
					s."SujuQty" AS suju_qty,
					s."Company_id" AS company_id,
					m."Name" AS material_name,
					SUM(sp."Qty") AS total_qty
				FROM suju s
				JOIN shipment sp ON s.id = sp."SourceDataPk"
				JOIN shipment_head sh ON sp."ShipmentHead_id" = sh.id
				LEFT JOIN material m ON m.id = s."Material_id"
				WHERE s."JumunDate" BETWEEN CAST(:date_from AS DATE) AND CAST(:date_to AS DATE)
				  AND sh."State" = 'shipped'
				  AND s."spjangcd" = :spjangcd
				GROUP BY
					s.id, s."JumunNumber", s."JumunDate", sh."ShipDate",
					s."SujuQty", s."Company_id", m."Name"
			),
				main_data AS (
				    SELECT
				        ss.jumun_number,
				        MIN(ss.jumundate) AS jumundate,
				        MAX(ss.shipdate) AS shipdate,
				        ROUND(AVG(ss.wday), 1) AS avg_wday,
				        SUM(ss.suju_qty) AS total_suju_qty,
				        SUM(ss.total_qty) AS total_shipped_qty,
				        c."Name" AS company_name,
				        ss.mon,
				        ss.yyyy,
				        CASE
				            WHEN COUNT(DISTINCT ss.material_name) = 1 THEN MAX(ss.material_name)
				            ELSE CONCAT(MAX(ss.material_name), ' 외 ', COUNT(DISTINCT ss.material_name) - 1, '건')
				        END AS material_summary
				    FROM suju_sum ss
				    LEFT JOIN company c ON c.id = ss.company_id
				    WHERE ss.suju_qty <= ss.total_qty
				    GROUP BY ss.jumun_number, c."Name", ss.mon, ss.yyyy
				)
				--  ① 실제 데이터
				SELECT
				    yyyy,
				    mon,
				    jumun_number,
				    company_name,
				    material_summary,
				    jumundate::text,
				    shipdate::text,
				    avg_wday,
				    total_suju_qty,
				    total_shipped_qty,
				    FALSE AS is_footer
				FROM main_data
		
				UNION ALL
			
				 --  ② 월별 합계 (푸터)
				 SELECT
				     yyyy,
				     mon,
				     CONCAT(yyyy, '년 ', LPAD(mon::text, 2, '0'), '월 합계') AS jumun_number,
				     NULL AS company_name,
				     NULL AS material_summary,
				     NULL AS jumundate,
				     CONCAT(LPAD(mon::text, 2, '0'), '월 평균납기일수') AS shipdate,  -- 텍스트 그대로
				     ROUND(AVG(avg_wday), 1) AS avg_wday,
				     SUM(total_suju_qty) AS total_suju_qty,
				     SUM(total_shipped_qty) AS total_shipped_qty,
				     TRUE AS is_footer
				 FROM main_data
				 GROUP BY yyyy, mon
			
				 ORDER BY yyyy, mon, is_footer, jumun_number;
			""");



		List<Map<String, Object>> rows = sqlRunner.getRows(sql.toString(), paramMap);

		AjaxResult result = new AjaxResult();
		result.data = rows;
		return result;
	}

}
