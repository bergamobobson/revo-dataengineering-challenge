-- =============================================================================
-- QUERY 2: Top 10 West Netherlands bike commuters (by km to work)
-- =============================================================================
-- "Top 10 users based in West Netherlands, who travelled the most by bike 
-- (in terms of km) to go to work"
-- =============================================================================

SELECT 
    dr.title AS region,
    dp.title AS population_segment,
    dm.title AS travel_mode,
    dtm.title AS travel_motive,
    SUM(f.distance_yearly) AS total_km
FROM 
    odin_mobility.fact_mobility f
    INNER JOIN odin_mobility.dim_regions dr ON f.region_key = dr.key
    INNER JOIN odin_mobility.dim_population dp ON f.population_key = dp.key
    INNER JOIN odin_mobility.dim_travel_modes dm ON f.travel_mode_key = dm.key
    INNER JOIN odin_mobility.dim_travel_motives dtm ON f.travel_motive_key = dtm.key
WHERE 
    dr.title ILIKE '%west%'                 -- Filter: West Netherlands
    AND dm.title ILIKE '%bike%'             -- Filter: by bike
    OR dm.title ILIKE '%bicycle%'
    AND (dtm.title ILIKE '%work%'           -- Filter: to go to work
         OR dtm.title ILIKE '%commut%')
GROUP BY 
    dr.title,
    dp.title,
    dm.title,
    dtm.title
ORDER BY 
    total_km DESC
LIMIT 10;