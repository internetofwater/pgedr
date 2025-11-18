-- Copyright 2025 Lincoln Institute of Land Policy
-- SPDX-License-Identifier: MIT

-- Clean slate
DROP TABLE IF EXISTS landing_observations;
DROP TABLE IF EXISTS edr_parameters;
DROP TABLE IF EXISTS edr_locations;
DROP TABLE IF EXISTS airports;

-- Airports (define WKT geometry)
CREATE TABLE airports (
    `code` VARCHAR(10) PRIMARY KEY,
    `name` VARCHAR(100),
    `city` VARCHAR(100),
    `state` VARCHAR(2)
);

-- EDR locations use airport code directly
CREATE TABLE airport_locations (
    `id` VARCHAR(10) PRIMARY KEY,  -- same as airport code
    `label` VARCHAR(100),
    `geometry_wkt` GEOMETRY NOT NULL,
    FOREIGN KEY (id) REFERENCES airports(code)
);

-- Parameter definitions
CREATE TABLE airport_parameters (
    `id` VARCHAR(50) PRIMARY KEY,
    `name` VARCHAR(50),
    `units` VARCHAR(50),
    `description` TEXT
);

-- Observations for EDR
CREATE TABLE landing_observations (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `location_id` VARCHAR(10) NOT NULL,  -- airport code
    `time` datetime NOT NULL,
    `parameter_id` VARCHAR(50) NOT NULL,
    `value` DOUBLE NOT NULL,
    `airline` VARCHAR(100),
    FOREIGN KEY (location_id) REFERENCES airports(code),
    FOREIGN KEY (parameter_id) REFERENCES airport_parameters(id)
);

-- Airports (used as EDR locations)
INSERT INTO airports (code, name, city, state) VALUES
('DCA', 'Ronald Reagan Washington National Airport', 'Washington', 'DC'),
('IAD', 'Washington Dulles International Airport', 'Dulles', 'VA'),
('BWI', 'Baltimore/Washington International Airport', 'Baltimore', 'MD'),
('CGS', 'College Park Airport', 'College Park', 'MD'),
('ADW', 'Joint Base Andrews Airport', 'Camp Springs', 'MD');

-- EDR locations (same IDs as airport codes)
INSERT INTO airport_locations (id, label, geometry_wkt) VALUES
('DCA', 'DCA Airport', ST_GeomFromText('POINT(-77.0377 38.8512)')),
('IAD', 'IAD Airport', ST_GeomFromText('POLYGON((-77.45594586507086 38.933037617532335,-77.45407483098975 38.93605300259193,-77.45602660009052 38.937283863319266,-77.45566652190276 38.94066987606965,-77.44013229676604 38.940966898034986,-77.44044958539338 38.92360692150481,-77.43594255291971 38.923611632298304,-77.43552555654306 38.95550783534881,-77.44037490550018 38.955546446471175,-77.44031596893579 38.95164316365998,-77.45483656590044 38.95165101187874,-77.45508200036028 38.970718361113626,-77.46026236289806 38.97082506808229,-77.46027891357711 38.95069352277582,-77.47167829468088 38.950928658183216,-77.47166255135204 38.970899135177774,-77.47530531544004 38.970955940821455,-77.47600165498477 38.94498316590898,-77.4606809739751 38.94428711702017,-77.46060064263052 38.93908552858446,-77.46125257791445 38.937815450389735,-77.48929467597516 38.94598845314343,-77.49063729935246 38.94313738124566,-77.45594586507086 38.933037617532335))')),
('BWI', 'BWI Airport', ST_GeomFromText('POINT(-76.6684 39.1754)')),
('CGS', 'CGS Airport', ST_GeomFromText('POLYGON((-76.9208174 38.9786553, -76.9201126 38.9779908, -76.9194596 38.9778419, -76.9187941 38.9773371, -76.9185381 38.977519, -76.9255778 38.9835916, -76.925732 38.9834639, -76.9254289 38.9832145, -76.9259106 38.9828487, -76.9265681 38.9814242, -76.9250388 38.9809225, -76.9248302 38.981018, -76.9245463 38.9812638, -76.9225129 38.9795235, -76.9221608 38.9797254, -76.9208174 38.9786553))')),
('ADW', 'ADW Airport', ST_GeomFromText('MULTIPOLYGON(((-76.8641386443654 38.79793241533346,-76.86310776771639 38.79789461713656,-76.86315663134249 38.82468765456031,-76.86454523349295 38.82455356387436,-76.8641386443654 38.79793241533346),(-76.8712755638091 38.825946971542294,-76.87013231852254 38.82590562547665,-76.86999105889551 38.79490362099014,-76.8713841022796 38.7948625679108,-76.8712755638091 38.825946971542294)))'));

-- Define a parameter: number of landings
INSERT INTO airport_parameters (id, name, units, description) VALUES
('landings', 'Daily plane landings', 'count', 'Number of planes landed'),
('crashes', 'Daily plane crashes', 'count', 'Number of plane crashes');

-- Landing observations grouped by airport and airline
INSERT INTO landing_observations (location_id, time, parameter_id, value, airline) VALUES
('DCA', '2025-04-30', 'landings', 89, 'American Airlines'),
('DCA', '2025-05-01', 'landings', 90, 'American Airlines'),
('DCA', '2025-05-02', 'landings', 85, 'American AirLines'),
('DCA', '2025-05-03', 'landings', 87, 'American AirLines'),
('DCA', '2025-05-04', 'landings', 88, 'American AirLines'),
('IAD', '2025-05-01', 'landings', 200, 'United Airlines'),
('IAD', '2025-05-02', 'landings', 50, 'United Airlines'),
('IAD', '2025-05-03', 'landings', 303, 'United Airlines'),
('IAD', '2025-05-01', 'crashes', 0, 'United Airlines'),
('IAD', '2025-05-02', 'crashes', 0, 'United Airlines'),
('BWI', '2025-05-01', 'landings', 41, 'Southwest Airlines'),
('BWI', '2025-05-02', 'landings', 40, 'Southwest Airlines'),
('BWI', '2025-05-03', 'landings', 40, 'Southwest Airlines'),
('BWI', '2025-05-01', 'crashes', 2, 'Southwest Airlines'),
('BWI', '2025-05-02', 'crashes', 9, 'Southwest Airlines'),
('ADW', '2025-04-03', 'landings', 2, 'Air Force One'),
('ADW', '2025-04-04', 'crashes', 1, 'Air Force One'),
('ADW', '2025-04-04', 'landings', 3, 'Air Force One');
