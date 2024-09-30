WITH
  station_data AS (
    -- 역별 이용량 계산 (시간 정보 제외)
    SELECT
      f_slot_sum.stnm,    -- 역 이름
      f_slot_sum.sidonm,  -- 시도 이름 (sidoname)
      f_slot_sum.ltid,    -- 호선 ID
      SUM(f_slot_sum.tot) AS station_total
    FROM
      f_slot_sum
      JOIN d_ym ON f_slot_sum.ymid = d_ym.ymid
    WHERE
      f_slot_sum.sidonm = {{sidonm}}  -- 선택한 시도 이름으로 필터링
      AND f_slot_sum.ltid = {{selected_ltid}} -- 선택된 호선으로 필터링
      AND d_ym.ymid = COALESCE(
        {{year_month}}, 
        TO_CHAR(DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month', 'YYYYMM')
      )
    GROUP BY
      f_slot_sum.stnm,
      f_slot_sum.sidonm,
      f_slot_sum.ltid
  ),
  ranked_station_data AS (
    SELECT
      ds.ltid,                    -- 호선 ID
      ds.stnm,                    -- 역 이름
      sd.sidonm,                  -- 시도 이름
      sd.station_total AS station_total,   -- 이용량
      ROW_NUMBER() OVER (PARTITION BY ds.ltid ORDER BY sd.station_total DESC) AS rn_desc -- 호선별 내림차순 순위
    FROM
      station_data sd
      JOIN d_st ds ON sd.stnm = ds.stnm
    WHERE ds.ltid = {{selected_ltid}}  -- 선택된 호선으로 필터링
  )
-- 선택된 역에 따라 결과 반환
SELECT
  CASE
    WHEN {{selected_stnm}} IS NULL THEN
      -- 역이 선택되지 않은 경우 호선의 전체 순위
      (SELECT ROW_NUMBER() OVER (ORDER BY SUM(station_total) DESC) AS line_rank
       FROM station_data
       GROUP BY ltid
       HAVING ltid = {{selected_ltid}})
    ELSE
      -- 역이 선택된 경우 해당 역의 호선 내 순위
      (SELECT rn_desc
       FROM ranked_station_data
       WHERE stnm = {{selected_stnm}} -- 선택된 역 필터링
         AND ltid = {{selected_ltid}} -- 선택된 호선 필터링
        )
  END AS rank_result
ORDER BY rank_result;
