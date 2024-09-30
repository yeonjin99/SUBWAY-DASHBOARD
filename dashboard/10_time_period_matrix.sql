WITH
  time_slot_data AS (
    -- 선택된 역의 시간대별 이용량 계산
    SELECT
      f_slot_sum.slotid, -- 시간대 ID
      SUM(f_slot_sum.tot) AS slot_total
    FROM
      f_slot_sum
      JOIN d_ym ON f_slot_sum.ymid = d_ym.ymid
      JOIN sido_cd ON f_slot_sum.sidonm = sido_cd.sidonm
    WHERE
      f_slot_sum.stnm = {{selected_stnm}} -- 선택된 역 이름
      AND f_slot_sum.ltid = {{selected_ltid}} -- 선택된 호선 ID
      AND (
        d_ym.ymid = {{year_option}}
      )
      AND f_slot_sum.slotid >= 4 -- 4시 이후의 시간대만 필터링
      AND sido_cd.sidonm = {{sidonm}}
    GROUP BY
      f_slot_sum.slotid
  )
SELECT
  ROUND(SUM(CASE WHEN tsd.slotid = 4 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "04시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 5 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "05시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 6 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "06시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 7 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "07시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 8 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "08시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 9 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "09시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 10 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "10시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 11 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "11시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 12 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "12시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 13 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "13시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 14 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "14시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 15 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "15시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 16 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "16시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 17 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "17시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 18 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "18시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 19 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "19시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 20 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "20시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 21 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "21시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 22 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "22시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 23 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "23시_백분율",
  ROUND(SUM(CASE WHEN tsd.slotid = 0 THEN tsd.slot_total ELSE 0 END) / SUM(slot_total) * 100, 2) AS "00시_백분율"
FROM
  time_slot_data tsd;
