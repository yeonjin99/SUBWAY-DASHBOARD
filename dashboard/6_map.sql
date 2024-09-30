WITH
  total_usage_last_month AS (
    -- 선택된 호선의 전체 이용량 계산 (지난달)
    SELECT
      SUM(f_st_sum.tot) AS total
    FROM
      f_st_sum
      JOIN d_ym ON f_st_sum.ymid = d_ym.ymid
      JOIN sido_cd ON f_st_sum.sidonm = sido_cd.sidonm
    WHERE
      f_st_sum.ltid = {{selected_ltid}} -- 선택된 호선 ID
      AND d_ym.ymid = {{year_option}}
      AND (
        sido_cd.sidonm = {{sidonm}}
      )
  ),
  total_usage_two_months_ago AS (
    -- 선택된 호선의 전체 이용량 계산 (지지난달)
    SELECT
      SUM(f_st_sum.tot) AS total
    FROM
      f_st_sum
      JOIN d_ym ON f_st_sum.ymid = d_ym.ymid
      JOIN sido_cd ON f_st_sum.sidonm = sido_cd.sidonm
    WHERE
      f_st_sum.ltid = {{selected_ltid}} -- 선택된 호선 ID
      AND (
        d_ym.year = CASE
          WHEN {{year_option}} = '동년전월' THEN CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS CHARACTER(4))
          ELSE CAST(EXTRACT(YEAR FROM CURRENT_DATE) - 1 AS CHARACTER(4))
        END
      )
      AND (
        d_ym.month = CASE
          WHEN {{year_option}} = '동년전월' THEN TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 2, 'FM00')
          ELSE TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 1, 'FM00')
        END
      )
      AND (
        sido_cd.sidonm = {{sidonm}}
      )
  ),
  station_data_last_month AS (
    -- 역별 이용량 계산 (지난달)
    SELECT
      f_st_sum.stnm, -- 역 이름름
      SUM(f_st_sum.tot) AS station_total
    FROM
      f_st_sum
      JOIN d_ym ON f_st_sum.ymid = d_ym.ymid
    WHERE
      f_st_sum.ltid = {{selected_ltid}} -- 선택된 호선 ID
      AND (
        d_ym.year = CASE
          WHEN {{year_option}} = '동년전월' THEN CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS CHARACTER(4))
          ELSE CAST(EXTRACT(YEAR FROM CURRENT_DATE) - 1 AS CHARACTER(4))
        END
      )
      AND (
        d_ym.month = CASE
          WHEN {{year_option}} = '동년전월' THEN CASE
            WHEN EXTRACT(DAY FROM CURRENT_DATE) <= 5 THEN TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 2, 'FM00')
            ELSE TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 1, 'FM00')
          END
          ELSE TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE), 'FM00')
        END
      )
    GROUP BY
      f_st_sum.stnm
  ),
  station_data_two_months_ago AS (
    -- 역별 이용량 계산 (지지난달)
    SELECT
      f_st_sum.stnm, -- 역 이름름
      SUM(f_st_sum.tot) AS station_total
    FROM
      f_st_sum
      JOIN d_ym ON f_st_sum.ymid = d_ym.ymid
      JOIN sido_cd ON f_st_sum.sidonm = sido_cd.sidonm
    WHERE
      f_st_sum.ltid = {{selected_ltid}} -- 선택된 호선 ID
      AND (
        d_ym.year = CASE
          WHEN {{year_option}} = '동년전월' THEN CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS CHARACTER(4))
          ELSE CAST(EXTRACT(YEAR FROM CURRENT_DATE) - 1 AS CHARACTER(4))
        END
      )
      AND (
        d_ym.month = CASE
          WHEN {{year_option}} = '동년전월' THEN CASE
            WHEN EXTRACT(DAY FROM CURRENT_DATE) <= 5 THEN TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 3, 'FM00')
            ELSE TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 2, 'FM00')
          END
          ELSE TO_CHAR(EXTRACT(MONTH FROM CURRENT_DATE) - 1, 'FM00')
        END
      )
    GROUP BY
      f_st_sum.stnm
  )
SELECT
  ds.ltid AS 호선,
  sdl.stnm AS 역명,
  ds.lat AS 위도, -- 위도
  ds.lot AS 경도, -- 경도
  sdl.station_total AS 이용량,
  sdt.station_total AS 이전이용량,
  ROUND(
    (sdl.station_total::numeric / sdt.station_total - 1) * 100, 2
  ) AS 증감율 -- 증감율
FROM
  station_data_last_month sdl
  LEFT JOIN station_data_two_months_ago sdt ON sdl.stnm = sdt.stnm
  JOIN d_st ds ON sdl.stnm = ds.stnm -- 역별 위경도 추가
WHERE ds.ltid = {{selected_ltid}}
ORDER BY
  이용량 DESC;
