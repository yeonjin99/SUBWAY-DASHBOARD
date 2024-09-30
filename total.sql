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
      d_ym.ymid = COALESCE(
        {{year_month}}, 
        TO_CHAR(DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month', 'YYYYMM')
      )
      AND sido_cd.sidonm = {{sidonm}} -- 선택된 시/도의 이름
  )
SELECT
  total_passengers AS total_usage -- 전체 호선의 이용객 합계
FROM
  total_usage;
