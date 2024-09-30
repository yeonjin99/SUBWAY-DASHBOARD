WITH
  total_usage AS (
    -- 선택된 시기에 맞는 모든 호선의 이용객 합계 계산
    SELECT
      SUM(f_st_sum.tot) AS total_passengers -- 전체 호선 이용객 합계
    FROM
      f_st_sum
      JOIN d_ym ON f_st_sum.ymid = d_ym.ymid
      JOIN sido_cd ON f_st_sum.sidonm = sido_cd.sidonm
    WHERE
      d_ym.year = CASE
        WHEN {{year_option}} = '동년전월' THEN CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS CHARACTER(4))
        ELSE CAST(EXTRACT(YEAR FROM CURRENT_DATE) - 1 AS CHARACTER(4))
      END
      AND d_ym.month = CASE
        WHEN {{year_option}} = '동년전월' THEN CASE
          WHEN EXTRACT(DAY FROM CURRENT_DATE) <= 5 THEN TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 2, 'FM00')
          ELSE TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 1, 'FM00')
        END
        ELSE TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE), 'FM00')
      END
      AND sido_cd.sidonm = {{sidonm}} -- 선택된 시/도의 이름
  )
SELECT
  total_passengers AS total_usage -- 전체 호선의 이용객 합계
FROM
  total_usage;
