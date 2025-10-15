-- Generic PostgreSQL/PostGIS schema for spatial monitoring data
-- SPDX-License-Identifier: MIT

-- Setup PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create supporting schemas
CREATE SCHEMA IF NOT EXISTS edr_quickstart;

-- Set ownership so we can access the schema
ALTER SCHEMA edr_quickstart OWNER TO postgres;

-- Generic "locations" table that represents monitoring locations
CREATE TABLE edr_quickstart.locations (
    location_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,

    -- allow for custom properties; useful for bringing in data from external sources
    -- and preserving any custom fields
    properties JSONB,
    
    geometry GEOMETRY
);

-- Generic "parameters" table that represents parameters for which one can request timeseries observations
CREATE TABLE edr_quickstart.parameters (
    parameter_id VARCHAR(50) PRIMARY KEY,
    parameter_name VARCHAR NOT NULL,
    
    parameter_unit_symbol VARCHAR(50),
    parameter_unit_label VARCHAR(255)
);

-- Generic "observations" table that represents timeseries observations
CREATE TABLE edr_quickstart.observations (
    observation_id SERIAL PRIMARY KEY,
    location_id INT NOT NULL REFERENCES edr_quickstart.locations(location_id),
    parameter_id VARCHAR(50) NOT NULL REFERENCES edr_quickstart.parameters(parameter_id),
    observation_value DOUBLE PRECISION,
    observation_time TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(location_id, parameter_id, observation_time)
);

-- Index for fast retrieval of recent observations by location & parameter
CREATE INDEX idx_observations_location_param_date 
    ON edr_quickstart.observations(location_id, parameter_id, observation_time DESC) 
    INCLUDE (observation_value);

CREATE INDEX idx_locations_geom 
    ON edr_quickstart.locations USING GIST (geometry);
