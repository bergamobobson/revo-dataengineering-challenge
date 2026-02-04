-- ============================================================
-- Create schema
-- ============================================================
CREATE SCHEMA IF NOT EXISTS odin_mobility;

-- ============================================================
-- DIMENSIONS (créer EN PREMIER)
-- ============================================================

CREATE TABLE IF NOT EXISTS odin_mobility.dim_travel_motives (
    key VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS odin_mobility.dim_population (
    key VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS odin_mobility.dim_travel_modes (
    key VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS odin_mobility.dim_margins (
    key VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS odin_mobility.dim_regions (
    key VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS odin_mobility.dim_periods (
    key VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);