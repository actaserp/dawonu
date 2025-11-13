package mes.app.definition.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class YearamtService {

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getYearamtList(
        String year, String ioflag, String cltid, String name, String endyn, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource()
            .addValue("year", year)
            .addValue("ioflag", ioflag)          // "0" or "1" (문자열로 사용)
            .addValue("searchid", cltid)
            .addValue("name", name)
            .addValue("endyn", endyn)            // "Y" / "N" (N이면 미마감 + NULL)
            .addValue("spjangcd", spjangcd);

        int targetYear = Integer.parseInt(year) - 1;
        String yyyymm = targetYear + "12";       // 전년도 12월
        dicParam.addValue("yyyymm", yyyymm);

        String sql;
        if ("0".equals(ioflag)) {
            // 매출
            sql = """
                    SELECT
                      c.id,
                      c."Name" AS company_name,
                      COALESCE(
                        COALESCE(y.yearamt, 0) + COALESCE(s.totalamt_sum, 0) - COALESCE(b.accin_sum, 0),
                        0
                      ) AS balance,
                      COALESCE(y.ioflag, :ioflag) AS ioflag,
                      :year || '12' AS yyyymm,
                      COALESCE(m.endyn, 'N') AS endyn
                    FROM company c
                    /* 전년도 12월 확정(개시잔액) */
                    LEFT JOIN (
                      SELECT cltcd, yearamt, ioflag
                      FROM tb_yearamt
                      WHERE yyyymm = :yyyymm
                        AND spjangcd = :spjangcd
                        AND ioflag   = :ioflag
                    ) y ON y.cltcd = c.id
                    /* 올해 마감 여부 */
                    LEFT JOIN (
                      SELECT cltcd, MAX(endyn) AS endyn
                      FROM tb_yearamt
                      WHERE yyyymm   = :year || '12'
                        AND ioflag   = :ioflag
                        AND spjangcd = :spjangcd
                      GROUP BY cltcd
                    ) m ON m.cltcd = c.id
                    /* 올해 매출 합계 */
                    LEFT JOIN (
                      SELECT cltcd, SUM(totalamt) AS totalamt_sum
                      FROM tb_salesment
                      WHERE misdate BETWEEN '20000101' AND :year || '1231'
                        AND spjangcd = :spjangcd
                      GROUP BY cltcd
                    ) s ON s.cltcd = c.id
                    LEFT JOIN (
                      SELECT cltcd, SUM(accin) AS accin_sum
                      FROM tb_banktransit
                      WHERE trdate BETWEEN '20000101' AND :year || '1231'
                        AND spjangcd = :spjangcd
                        AND ioflag = '0'
                      GROUP BY cltcd
                    ) b ON b.cltcd = c.id
                    WHERE c.relyn = '0'
                      AND c.id::text LIKE concat('%', :searchid, '%')
                      AND c."Name"  LIKE concat('%', :name, '%')
                      AND c.spjangcd = :spjangcd
                      AND COALESCE(m.endyn, 'N') = :endyn
                    ORDER BY c."Name"
                """;
        } else {
            // 매입
            sql = """
                    WITH client AS (
                         SELECT id, '0' AS cltflag, "Name" AS cltname
                         FROM company WHERE spjangcd = :spjangcd
                         UNION ALL
                         SELECT id, '1' AS cltflag, "Name" AS cltname
                         FROM person WHERE spjangcd = :spjangcd
                         UNION ALL
                         SELECT bankid AS id, '2' AS cltflag, banknm AS cltname
                         FROM tb_xbank WHERE spjangcd = :spjangcd
                         UNION ALL
                         SELECT id, '3' AS cltflag, cardnm AS cltname
                         FROM tb_iz010 WHERE spjangcd = :spjangcd
                     ),
                     yearamt AS (
                         SELECT cltcd, yearamt, ioflag
                         FROM tb_yearamt
                         WHERE yyyymm = :yyyymm
                           AND spjangcd = :spjangcd
                     ),
                     end_flag AS (
                         SELECT cltcd, MAX(endyn) AS endyn
                         FROM tb_yearamt
                         WHERE yyyymm   = :year || '12'
                           AND ioflag   = :ioflag
                           AND spjangcd = :spjangcd
                         GROUP BY cltcd
                     ),
                     invo_sum AS (
                         SELECT cltcd, SUM(totalamt) AS totalamt_sum
                         FROM tb_invoicement
                         WHERE misdate BETWEEN '20000101' AND :year || '1231'
                           AND spjangcd = :spjangcd
                         GROUP BY cltcd
                     ),
                     bank_sum AS (
                         SELECT cltcd, SUM(accout) AS accout_sum
                         FROM tb_banktransit
                         WHERE trdate BETWEEN '20000101' AND :year || '1231'
                           AND spjangcd = :spjangcd
                           AND ioflag = '1'
                         GROUP BY cltcd
                     )
                     SELECT
                         c.id,
                         c.cltflag,
                         CASE c.cltflag
                               WHEN '0' THEN '업체'
                               WHEN '1' THEN '직원정보'
                               WHEN '2' THEN '은행계좌'
                               WHEN '3' THEN '카드사'
                         END AS cltflagnm,
                         c.cltname AS company_name,
                         COALESCE(
                             COALESCE(y.yearamt, 0) + COALESCE(s.totalamt_sum, 0) - COALESCE(b.accout_sum, 0),
                             0
                         ) AS balance,
                         COALESCE(y.ioflag, :ioflag) AS ioflag,
                         :year || '12' AS yyyymm,
                         COALESCE(m.endyn, 'N') AS endyn
                     FROM client c
                     LEFT JOIN yearamt y  ON y.cltcd = c.id
                     LEFT JOIN end_flag m ON m.cltcd = c.id
                     LEFT JOIN invo_sum s ON s.cltcd = c.id
                     LEFT JOIN bank_sum b ON b.cltcd = c.id
                     WHERE c.id::text LIKE concat('%', :searchid, '%')
                       AND c.cltname LIKE concat('%', :name, '%')
                       AND COALESCE(m.endyn, 'N') = :endyn
                     ORDER BY c.cltflag,  c.cltname
                """;
        }

//        log.info("매입매출 년마감 SQL: {}", sql);
//        log.info("SQL Parameters: {}", dicParam.getValues());

        return sqlRunner.getRows(sql, dicParam);
    }


//    public List<Map<String, Object>> getYearamtList(
//        String year, String ioflag, String cltid, String name, String endyn, String spjangcd) {
//
//        MapSqlParameterSource dicParam = new MapSqlParameterSource()
//            .addValue("year", year)
//            .addValue("ioflag", ioflag)      // "0"=매출, "1"=매입
//            .addValue("searchid", cltid)
//            .addValue("name", name)
//            .addValue("endyn", endyn)        // "Y" / "N" / (선택) "ALL"
//            .addValue("spjangcd", spjangcd);
//
//        int targetYear = Integer.parseInt(year) - 1;
//        String yyyymm = targetYear + "12";   // 전년도 12월
//        dicParam.addValue("yyyymm", yyyymm);
//
//        String sql;
//        if ("0".equals(ioflag)) {
//            // ===== 매출 =====
//            sql = """
//      SELECT
//        c.id,
//        c."Name" AS company_name,
//
//        /* 디버깅/설명용 구성요소 */
//        COALESCE(y.yearamt, 0)                  AS opening_prev_dec,   -- 전년도 12월 개시
//        COALESCE(s.totalamt_sum, 0)             AS sales_sum,          -- 매출 합계
//        COALESCE(b.accin_sum, 0)                AS receipts_sum,       -- 입금 합계
//
//        /* 마감 확정 금액(해당년도 12월 yearamt) */
//        y2.closed_yearamt,
//
//        /* 마감전금액(계산) */
//        (COALESCE(y.yearamt,0) + COALESCE(s.totalamt_sum,0) - COALESCE(b.accin_sum,0))
//          AS balance_preclose,
//
//        /* 표시 잔액: 마감이면 확정값, 아니면 계산값 */
//        CASE
//          WHEN COALESCE(m.endyn,'N') = 'Y' AND y2.closed_yearamt IS NOT NULL
//            THEN y2.closed_yearamt
//          ELSE COALESCE(y.yearamt,0) + COALESCE(s.totalamt_sum,0) - COALESCE(b.accin_sum,0)
//        END AS balance_final,
//
//        COALESCE(y.ioflag, :ioflag) AS ioflag,
//        :year || '12' AS yyyymm,
//        COALESCE(m.endyn, 'N') AS endyn
//      FROM company c
//
//      /* 전년도 12월 확정(개시잔액) */
//      LEFT JOIN (
//        SELECT cltcd, yearamt, ioflag
//        FROM tb_yearamt
//        WHERE yyyymm   = :yyyymm
//          AND spjangcd = :spjangcd
//          AND ioflag   = :ioflag
//      ) y ON y.cltcd = c.id
//
//      /* 올해 마감 여부 */
//      LEFT JOIN (
//        SELECT cltcd, MAX(endyn) AS endyn
//        FROM tb_yearamt
//        WHERE yyyymm   = :year || '12'
//          AND ioflag   = :ioflag
//          AND spjangcd = :spjangcd
//        GROUP BY cltcd
//      ) m ON m.cltcd = c.id
//
//      /* 올해 12월 마감 확정 금액(yearamt) */
//      LEFT JOIN (
//        SELECT cltcd, yearamt AS closed_yearamt
//        FROM tb_yearamt
//        WHERE yyyymm   = :year || '12'
//          AND ioflag   = :ioflag
//          AND spjangcd = :spjangcd
//          AND endyn    = 'Y'
//      ) y2 ON y2.cltcd = c.id
//
//      /* 올해 매출 합계 (문자형 날짜/선행0 대비) */
//      LEFT JOIN (
//        WITH raw AS (
//          SELECT
//            TRIM(LEADING '0' FROM cltcd::text)      AS cltcd_norm,
//            totalamt,
//            REGEXP_REPLACE(misdate,'[^0-9]','','g') AS ymd8
//          FROM tb_salesment
//          WHERE spjangcd = :spjangcd
//        )
//        SELECT cltcd_norm, SUM(totalamt) AS totalamt_sum
//        FROM raw
//        WHERE ymd8 ~ '^[0-9]{8}$'
//          AND TO_DATE(ymd8,'YYYYMMDD')
//              BETWEEN DATE '2000-01-01' AND TO_DATE(:year || '1231','YYYYMMDD')
//        GROUP BY cltcd_norm
//      ) s ON s.cltcd_norm = TRIM(LEADING '0' FROM c.id::text)
//
//      /* 올해 입금 합계(매출쪽) (문자형 날짜/선행0 대비) */
//      LEFT JOIN (
//        WITH raw AS (
//          SELECT
//            TRIM(LEADING '0' FROM cltcd::text)      AS cltcd_norm,
//            accin,
//            REGEXP_REPLACE(trdate,'[^0-9]','','g')  AS ymd8
//          FROM tb_banktransit
//          WHERE spjangcd = :spjangcd
//            AND ioflag   = :ioflag   -- '0'
//        )
//        SELECT cltcd_norm, SUM(accin) AS accin_sum
//        FROM raw
//        WHERE ymd8 ~ '^[0-9]{8}$'
//          AND TO_DATE(ymd8,'YYYYMMDD')
//              BETWEEN DATE '2000-01-01' AND TO_DATE(:year || '1231','YYYYMMDD')
//        GROUP BY cltcd_norm
//      ) b ON b.cltcd_norm = TRIM(LEADING '0' FROM c.id::text)
//
//      WHERE c.relyn = '0'
//        AND c.id::text LIKE CONCAT('%', :searchid, '%')
//        AND c."Name"   LIKE CONCAT('%', :name, '%')
//        AND c.spjangcd = :spjangcd
//        AND (
//          :endyn = 'ALL' OR COALESCE(m.endyn,'N') = :endyn
//        )
//      ORDER BY c."Name";
//      """;
//        } else {
//            // ===== 매입 =====
//            sql = """
//      WITH client AS (
//        SELECT id, '0' AS cltflag, "Name" AS cltname
//        FROM company WHERE spjangcd = :spjangcd
//        UNION ALL
//        SELECT id, '1' AS cltflag, "Name" AS cltname
//        FROM person WHERE spjangcd = :spjangcd
//        UNION ALL
//        SELECT bankid AS id, '2' AS cltflag, banknm AS cltname
//        FROM tb_xbank WHERE spjangcd = :spjangcd
//        UNION ALL
//        SELECT id, '3' AS cltflag, cardnm AS cltname
//        FROM tb_iz010 WHERE spjangcd = :spjangcd
//      ),
//      /* 전년도 12월 개시 */
//      year_open AS (
//        SELECT cltcd, yearamt, ioflag
//        FROM tb_yearamt
//        WHERE yyyymm = :yyyymm
//          AND spjangcd = :spjangcd
//          AND ioflag   = :ioflag       -- '1'
//      ),
//      /* 올해 마감 여부 */
//      end_flag AS (
//        SELECT cltcd, MAX(endyn) AS endyn
//        FROM tb_yearamt
//        WHERE yyyymm   = :year || '12'
//          AND ioflag   = :ioflag       -- '1'
//          AND spjangcd = :spjangcd
//        GROUP BY cltcd
//      ),
//      /* 올해 12월 마감 확정 금액 */
//      year_closed AS (
//        SELECT cltcd, yearamt AS closed_yearamt
//        FROM tb_yearamt
//        WHERE yyyymm   = :year || '12'
//          AND ioflag   = :ioflag       -- '1'
//          AND spjangcd = :spjangcd
//          AND endyn    = 'Y'
//      ),
//      /* 매입 합계 (문자형 날짜 정규화) */
//      invo_sum AS (
//        WITH raw AS (
//          SELECT
//            cltcd,
//            totalamt,
//            REGEXP_REPLACE(misdate,'[^0-9]','','g') AS ymd8
//          FROM tb_invoicement
//          WHERE spjangcd = :spjangcd
//        )
//        SELECT cltcd, SUM(totalamt) AS totalamt_sum
//        FROM raw
//        WHERE ymd8 ~ '^[0-9]{8}$'
//          AND TO_DATE(ymd8,'YYYYMMDD')
//              BETWEEN DATE '2000-01-01' AND TO_DATE(:year || '1231','YYYYMMDD')
//        GROUP BY cltcd
//      ),
//      /* 지급 합계 (문자형 날짜 정규화) */
//      bank_sum AS (
//        WITH raw AS (
//          SELECT
//            cltcd,
//            accout,
//            REGEXP_REPLACE(trdate,'[^0-9]','','g') AS ymd8
//          FROM tb_banktransit
//          WHERE spjangcd = :spjangcd
//            AND ioflag   = :ioflag   -- '1'
//        )
//        SELECT cltcd, SUM(accout) AS accout_sum
//        FROM raw
//        WHERE ymd8 ~ '^[0-9]{8}$'
//          AND TO_DATE(ymd8,'YYYYMMDD')
//              BETWEEN DATE '2000-01-01' AND TO_DATE(:year || '1231','YYYYMMDD')
//        GROUP BY cltcd
//      )
//      SELECT
//        c.id,
//        c.cltflag,
//        CASE c.cltflag
//          WHEN '0' THEN '업체'
//          WHEN '1' THEN '직원정보'
//          WHEN '2' THEN '은행계좌'
//          WHEN '3' THEN '카드사'
//        END AS cltflagnm,
//        c.cltname AS company_name,
//
//        /* 디버깅/설명용 */
//        COALESCE(y.yearamt, 0)              AS opening_prev_dec,   -- 전년도 12월 개시(매입)
//        COALESCE(s.totalamt_sum, 0)         AS purchase_sum,       -- 매입 합계
//        COALESCE(b.accout_sum, 0)           AS payments_sum,       -- 지급 합계
//
//        /* 마감 확정 금액 */
//        yc.closed_yearamt,
//
//        /* 마감전금액(계산) */
//        (COALESCE(y.yearamt,0) + COALESCE(s.totalamt_sum,0) - COALESCE(b.accout_sum,0))
//          AS balance_preclose,
//
//        /* 표시 잔액: 마감이면 확정값, 아니면 계산값 */
//        CASE
//          WHEN COALESCE(m.endyn,'N') = 'Y' AND yc.closed_yearamt IS NOT NULL
//            THEN yc.closed_yearamt
//          ELSE COALESCE(y.yearamt,0) + COALESCE(s.totalamt_sum,0) - COALESCE(b.accout_sum,0)
//        END AS balance_final,
//
//        :ioflag AS ioflag,
//        :year || '12' AS yyyymm,
//        COALESCE(m.endyn, 'N') AS endyn
//      FROM client c
//      LEFT JOIN year_open  y  ON y.cltcd = c.id
//      LEFT JOIN end_flag   m  ON m.cltcd = c.id
//      LEFT JOIN year_closed yc ON yc.cltcd = c.id
//      LEFT JOIN invo_sum   s  ON s.cltcd = c.id
//      LEFT JOIN bank_sum   b  ON b.cltcd = c.id
//      WHERE c.id::text LIKE CONCAT('%', :searchid, '%')
//        AND c.cltname  LIKE CONCAT('%', :name, '%')
//        AND (
//          :endyn = 'ALL' OR COALESCE(m.endyn,'N') = :endyn
//        )
//      ORDER BY c.cltflag, c.cltname;
//      """;
//        }
//
////        log.info("매입매출 년마감 SQL: {}", sql);
////        log.info("SQL Parameters: {}", dicParam.getValues());
//        return sqlRunner.getRows(sql, dicParam);
//    }
}

