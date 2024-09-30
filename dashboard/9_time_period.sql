WITH
  time_slot_data AS (
    -- 시간대별 이용량 계산 (역이 선택된 경우 해당 역, 그렇지 않으면 호선 전체 또는 '서울' 기본값)
    SELECT
      f_slot_sum.slotid, -- 시간대 ID
      SUM(f_slot_sum.tot) AS slot_total
    FROM
      f_slot_sum
      JOIN d_ym ON f_slot_sum.ymid = d_ym.ymid
      JOIN sido_cd ON f_slot_sum.sidonm = sido_cd.sidonm
    WHERE
      f_slot_sum.ltid = {{selected_ltid}} -- 선택된 호선 ID
      AND d_ym.ymid = {{year_option}} -- Metabase에서 선택한 year_option 값으로 필터링
      
      -- 역이 선택되지 않았을 경우 '서울'을 기본값으로 설정 (변수 처리)
      AND f_slot_sum.stnm = {{selected_stnm}}
      AND f_slot_sum.slotid >= 4 -- 4시 이후의 시간대만 필터링
      AND sido_cd.sidonm = {{sidonm}} -- 선택된 시도 이름으로 필터링
    GROUP BY
      f_slot_sum.slotid
  )
SELECT
  tsd.slotid AS 시간,
  tsd.slot_total AS 이용량
FROM
  time_slot_data tsd
ORDER BY
  CASE
    WHEN tsd.slotid >= 4 THEN tsd.slotid  -- 4시부터 23시까지 정상적으로 정렬
    WHEN tsd.slotid = 0 THEN 24  -- 0시를 맨 마지막에 정렬
  END;
