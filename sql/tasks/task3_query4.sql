-- =============================================================================
-- QUERY 4: Average trips per year for top 10 least-traveling education segments
-- =============================================================================
-- "Among the top 10 users who spend the least number of hours travelling to 
-- attend education/courses, show for every year the average number of trips 
-- made by these users"
-- =============================================================================

WITH least_travel_education AS (
    -- Step 1: Find top 10 segments who spend least hours on education travel
    SELECT 
        f.population_key,
        f.region_key,
        SUM(f.time_yearly) AS total_hours
    FROM 
        odin_mobility.fact_mobility f
        INNER JOIN odin_mobility.dim_travel_motives dtm ON f.travel_motive_key = dtm.key
    WHERE 
        dtm.title ILIKE '%education%'
        OR dtm.title ILIKE '%course%'
        OR dtm.title ILIKE '%school%'
    GROUP BY 
        f.population_key,
        f.region_key
    HAVING 
        SUM(f.time_yearly) > 0              -- Exclude zeros
    ORDER BY 
        total_hours ASC
    LIMIT 10
)
-- Step 2: Get average trips per year for these segments
SELECT 
    per.title AS year,
    ROUND(AVG(f.trips_yearly), 2) AS avg_trips
FROM 
    odin_mobility.fact_mobility f
    INNER JOIN odin_mobility.dim_periods per ON f.period_key = per.key
    INNER JOIN least_travel_education lte 
        ON f.population_key = lte.population_key 
        AND f.region_key = lte.region_key
GROUP BY 
    per.title
ORDER BY 
    per.title;
