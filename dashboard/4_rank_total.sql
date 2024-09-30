WITH
  station_data AS (
    -- 역별 7-9시 시간대 이용량 계산
    SELECT
      f_slot_sum.stnm,    -- 역 이름
      f_slot_sum.sidonm,  -- 시도 이름 (sidoname)
      SUM(f_slot_sum.tot)/2 AS station_total
    FROM
      f_slot_sum
      JOIN d_ym ON f_slot_sum.ymid = d_ym.ymid
    WHERE
      f_slot_sum.slotid IN (7, 8) -- 출근 시간대 (7-9시)
      AND f_slot_sum.sidonm = {{sidonm}}  -- 선택한 시도 이름으로 필터링
      AND d_ym.ymid = COALESCE(
        {{year_month}},  -- 사용자가 선택한 year_month 값
        TO_CHAR(DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month', 'YYYYMM')  -- 기본값으로 지난달 설정
      )
    GROUP BY
      f_slot_sum.stnm,
      f_slot_sum.sidonm
  ),
  ranked_station_data AS (
    SELECT
      ds.ltid,                    -- 호선 ID
      sd.stnm AS station_id,       -- 역 이름
      sd.sidonm,                  -- 시도 코드
      sd.station_total AS station_total,   -- 시간대 이용량
      ROW_NUMBER() OVER (ORDER BY sd.station_total DESC) AS rn_desc -- 내림차순 순위
    FROM
      station_data sd
      JOIN d_st ds ON sd.stnm = ds.stnm
  )
-- 최종 SELECT 쿼리로 상위 5개 역 필터링
SELECT
  *
FROM ranked_station_data
ORDER BY rn_desc;
