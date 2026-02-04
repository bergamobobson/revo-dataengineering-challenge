-- =============================================================================
-- QUERY 1: Total trips by travel method and urbanization (grocery shopping)
-- =============================================================================
-- "Total number of trips across all the years, grouped by travel method and 
-- by level of urbanization for people who went shopping for groceries"
-- =============================================================================

SELECT 
    dm.title AS travel_mode,
    dr.title AS urbanization_level,
    SUM(f.trips_yearly) AS total_trips
FROM 
    odin_mobility.fact_mobility f
    INNER JOIN odin_mobility.dim_travel_motives dtm ON f.travel_motive_key = dtm.key
    INNER JOIN odin_mobility.dim_travel_modes dm ON f.travel_mode_key = dm.key
    INNER JOIN odin_mobility.dim_regions dr ON f.region_key = dr.key
WHERE 
    dtm.title ILIKE '%groceries%'           -- Filter: grocery shopping
    OR dtm.title ILIKE '%shopping%'
GROUP BY 
    dm.title,
    dr.title
ORDER BY 
    total_trips DESC;