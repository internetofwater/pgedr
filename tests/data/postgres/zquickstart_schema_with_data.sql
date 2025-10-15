-- Sample locations
INSERT INTO edr_quickstart.locations 
    (name, description, properties, geometry)
VALUES
('Central Park', 'Urban park in NYC', '{"type":"park","area_ha":341}', ST_SetSRID(ST_Point(-73.9654, 40.7829), 4326)),
('Golden Gate Bridge', 'Famous suspension bridge', '{"type":"bridge","length_m":2737}', ST_SetSRID(ST_Point(-122.4783, 37.8199), 4326)),
('Millennium Park', 'Park with Cloud Gate sculpture', '{"type":"park","area_ha":25}', ST_SetSRID(ST_Point(-87.6226, 41.8826), 4326));

-- Sample parameters
INSERT INTO edr_quickstart.parameters 
    (parameter_id, parameter_name, parameter_unit_symbol, parameter_unit_label)
VALUES
('TEMP', 'Temperature', '°C', 'Celsius'),
('AQI', 'Air Quality Index', 'AQI', 'Air Quality Index'),
('HUM', 'Relative Humidity', '%', 'Percent');

-- Sample observations
INSERT INTO edr_quickstart.observations 
    (location_id, parameter_id, observation_value, observation_time)
VALUES
-- Central Park
(1, 'TEMP', 22.5, '2025-09-10 09:00:00+00'::timestamptz),
(1, 'AQI', 42, '2025-09-10 09:00:00+00'::timestamptz),
(1, 'HUM', 55, '2025-09-10 09:00:00+00'::timestamptz),

-- Golden Gate Bridge
(2, 'TEMP', 18.2, '2025-09-10 09:00:00+00'::timestamptz),
(2, 'AQI', 58, '2025-09-10 09:00:00+00'::timestamptz),
(2, 'HUM', 70, '2025-09-10 09:00:00+00'::timestamptz),

-- Millennium Park
(3, 'TEMP', 25.0, '2025-09-10 09:00:00+00'::timestamptz),
(3, 'AQI', 35, '2025-09-10 09:00:00+00'::timestamptz),
(3, 'HUM', 60, '2025-09-10 09:00:00+00'::timestamptz);
