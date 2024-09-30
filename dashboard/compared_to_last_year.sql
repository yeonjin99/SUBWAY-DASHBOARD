WITH selected_month_data AS(
    SELECT 
        d.ymid,
        SUM(tot) as total_usage
    FROM 
        f_lt_sum f
    JOIN 
        d_ym d ON f.ymid = d.ymid
    WHERE 
        d.ymid IN (
            (SELECT ymid FROM d_ym WHERE ymid = {{ymid}}),
            (SELECT ymid FROM d_ym WHERE year = CAST(CAST(SUBSTRING({{ymid}}, 1, 4) AS INT) - 1 AS TEXT) 
                AND month = SUBSTRING({{ymid}}, 5, 2))
        )
    GROUP BY d.ymid
)
SELECT
    {{ymid}} AS 기준년월,
    CONCAT(CAST(SUBSTRING({{ymid}}, 1, 4) AS INT) - 1, SUBSTRING({{ymid}}, 5, 2)) AS 작년, 
    SUM(CASE WHEN ymid = {{ymid}} THEN total_usage END) AS 올해이용량,
    SUM(CASE WHEN ymid = CONCAT(CAST(CAST(SUBSTRING({{ymid}}, 1, 4) AS INT) - 1 AS TEXT), SUBSTRING({{ymid}}, 5, 2)) THEN total_usage END) AS 작년이용량
FROM
    selected_month_data
GROUP BY
    기준년월, 작년;
