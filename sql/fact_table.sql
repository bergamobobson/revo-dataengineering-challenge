-- Drop and recreate fact table with correct types

CREATE TABLE IF NOT EXISTS odin_mobility.fact_mobility (
    fact_id CHAR(32) PRIMARY KEY,
    

    travel_motive_key VARCHAR(20) REFERENCES odin_mobility.dim_travel_motives(key),
    population_key VARCHAR(20) REFERENCES odin_mobility.dim_population(key),
    travel_mode_key VARCHAR(20) REFERENCES odin_mobility.dim_travel_modes(key),
    margin_key VARCHAR(20) REFERENCES odin_mobility.dim_margins(key),
    region_key VARCHAR(20) REFERENCES odin_mobility.dim_regions(key),
    period_key VARCHAR(20) REFERENCES odin_mobility.dim_periods(key),
    

    trips_daily NUMERIC(10,2),
    distance_daily NUMERIC(10,2),
    time_daily NUMERIC(10,2),

   
    trips_yearly NUMERIC(12,2),
    distance_yearly NUMERIC(12,2),
    time_yearly NUMERIC(10,2),
    
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);