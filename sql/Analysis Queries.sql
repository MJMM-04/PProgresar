------- La temperatura con la que inicio el día.
WITH cte AS (
    SELECT
        a.date,
        dc2.country,
        dc.city,
        dw.weather,
        a.temp,
        a.feelslike,
        a.pressure,
        a.humidity,
        ROW_NUMBER() OVER (
            PARTITION BY a.cityid, a.date
            ORDER BY a.created_at
        ) AS row_num
    FROM fact_weather a
    INNER JOIN dim_weather dw ON a.weatherid = dw.weatherid
    INNER JOIN dim_city dc ON dc.cityid = a.cityid
    INNER JOIN dim_country dc2 ON dc2.countryid = dc.countryid
)
SELECT *
FROM cte
WHERE row_num = 1;

-------- Temperatura maxima, minima y promedio por día y ciudad

SELECT
    a.date,
    dc2.country,
    dc.city,
    AVG(a.temp) AS avg_temp,
    MIN(a.temp) AS min_temp,
    MAX(a.temp) AS max_temp
FROM fact_weather a
INNER JOIN dim_weather dw ON a.weatherid = dw.weatherid
INNER JOIN dim_city dc ON dc.cityid = a.cityid
INNER JOIN dim_country dc2 ON dc2.countryid = dc.countryid
GROUP BY
    a.date,
    dc2.country,
    dc.city;

