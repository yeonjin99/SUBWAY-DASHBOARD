WITH
  total_usage AS (
    -- 1호선의 전체 이용량 계산
    SELECT
      SUM(f_st_sum.tot) AS total
    FROM
      f_st_sum
      JOIN d_ym ON f_st_sum.ymid = d_ym.ymid
      JOIN sido_cd ON f_st_sum.sidonm = sido_cd.sidonm
    WHERE
      f_st_sum.ltid = 1  -- 1호선으로 고정
      AND d_ym.ymid = {{year_option}}
      AND sido_cd.sidonm = {{sidonm}}
  ),
  station_data AS (
    -- 역별 이용량 계산
    SELECT
      f_st_sum.stnm, -- 역 이름
      SUM(f_st_sum.tot) AS station_total
    FROM
      f_st_sum
      JOIN d_ym ON f_st_sum.ymid = d_ym.ymid
    WHERE
      f_st_sum.ltid = 1  -- 1호선으로 고정
      AND d_ym.ymid = {{year_option}}
    GROUP BY
      f_st_sum.stnm
  ),
  ranked_station_data AS (
    SELECT
      ds.ltid,  -- LTID 추가
      sd.stnm AS station_nm,
      ds.lat,  -- 위도
      ds.lot,  -- 경도
      sd.station_total AS station_total,
      (sd.station_total::numeric / tu.total * 100) AS percentage_of_total,
      ROW_NUMBER() OVER (ORDER BY sd.station_total DESC) AS rn_desc,  -- 내림차순 순위
      ROW_NUMBER() OVER (ORDER BY sd.station_total ASC) AS rn_asc     -- 오름차순 순위
    FROM
      station_data sd
      CROSS JOIN total_usage tu
      JOIN d_st ds ON sd.stnm = ds.stnm -- 역별 위경도 추가
    WHERE ds.ltid = 1  -- 1호선으로 고정
  )
SELECT
  ltid,  -- LTID 추가
  station_nm AS 역명,
  lat,
  lot,
  station_total,
  percentage_of_total,
  rn_desc AS rank
FROM ranked_station_data
ORDER BY rn_desc;
