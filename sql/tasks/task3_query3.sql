-- =============================================================================
-- QUERY 3: Least common travel reasons for top 8 bike travelers (2022)
-- =============================================================================
-- "Among the top 8 users who travel the most km by bike, show the 3 least 
-- common reasons for travelling in year 2022"
-- =============================================================================

WITH top_8_bike_travelers AS (
    -- Step 1: Find top 8 segments by bike km
    SELECT 
        f.population_key,
        f.region_key,
        SUM(f.distance_yearly) AS total_bike_km
    FROM 
        odin_mobility.fact_mobility f
        INNER JOIN odin_mobility.dim_travel_modes dm ON f.travel_mode_key = dm.key
    WHERE 
        dm.title ILIKE '%bike%' 
        OR dm.title ILIKE '%bicycle%'
    GROUP BY 
        f.population_key,
        f.region_key
    ORDER BY 
        total_bike_km DESC
    LIMIT 8
),
travel_reasons_2022 AS (
    -- Step 2: Get travel reasons for these segments in 2022
    SELECT 
        dtm.title AS travel_reason,
        SUM(f.trips_yearly) AS total_trips
    FROM 
        odin_mobility.fact_mobility f
        INNER JOIN odin_mobility.dim_travel_motives dtm ON f.travel_motive_key = dtm.key
        INNER JOIN odin_mobility.dim_periods per ON f.period_key = per.key
        INNER JOIN top_8_bike_travelers t8 
            ON f.population_key = t8.population_key 
            AND f.region_key = t8.region_key
    WHERE 
        per.title ILIKE '%2022%'
    GROUP BY 
        dtm.title
)
-- Step 3: Get 3 least common reasons
SELECT 
    travel_reason,
    total_trips
FROM 
    travel_reasons_2022
WHERE 
    total_trips IS NOT NULL
ORDER BY 
    total_trips ASC
LIMIT 3;