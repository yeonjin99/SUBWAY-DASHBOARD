WITH selected_month_data AS(
    SELECT 
        d.ymid,
        SUM(tot) AS total_usage
    FROM 
        f_lt_sum f
    JOIN 
        d_ym d ON f.ymid = d.ymid
    WHERE 
        d.ymid IN (
            -- 기준년월 ymid
            (SELECT ymid FROM d_ym WHERE ymid = {{ymid}}),
            -- 기준년월에서 한 달 전 ymid
            (SELECT ymid FROM d_ym 
             WHERE year = SUBSTRING({{ymid}}, 1, 4)  -- 기준년월의 연도 그대로
             AND month = TO_CHAR(CAST(SUBSTRING({{ymid}}, 5, 2) AS INT) - 1, 'FM00'))  -- 한 달 전의 월
        )
    GROUP BY d.ymid
)
SELECT
    {{ymid}} AS 기준년월,  -- 입력된 기준년월 (YYYYMM)
    CONCAT(SUBSTRING({{ymid}}, 1, 4), TO_CHAR(CAST(SUBSTRING({{ymid}}, 5, 2) AS INT) - 1, 'FM00')) AS 전월,  -- 기준년월에서 한 달 전 (YYYYMM)
    SUM(CASE WHEN ymid = {{ymid}} THEN total_usage END) AS 이번달이용량,  -- 이번달 이용량
    SUM(CASE WHEN ymid = CONCAT(SUBSTRING({{ymid}}, 1, 4), TO_CHAR(CAST(SUBSTRING({{ymid}}, 5, 2) AS INT) - 1, 'FM00')) THEN total_usage END) AS 전월이용량  -- 전월 이용량
FROM
    selected_month_data
GROUP BY
    기준년월, 전월;
