# pygeoapi Environmental Data Retrieval

This repository contains SQL [pygeoapi](https://github.com/geopython/pygeoapi) providers for OGC API - Environmental Data Retrieval (EDR).

## OGC API - EDR

The configuration for SQL OGC API - EDR follows that of pygeoapi for [OGC API - Features](https://docs.pygeoapi.io/en/latest/data-publishing/ogcapi-features.html#postgresql), with the addition of two sections `edr_fields` and `external_tables`.
For more detailed documentation on the creation of a pygeoapi configuration file, refer
to the [docs](https://docs.pygeoapi.io/en/latest/configuration.html).

## Config Explanation

- The `table` field represents the top level table that all joins are relative to
- The `edr_fields` section defines the columns of your SQL table and their corresponding field in OGC API - EDR.
    - Fields are defined in the format `TABLE.COLUMN`
- The `external_tables` section allows foriegn table joins to allow `edr_fields` to refer to any table/column with a mapped relationship to the primary table. 
  - `foreign` is the column in the primary table defined in the `table` field 
  - `remote` is the column in the foreign table defined in the `external_tables`
      - Example: The config below describes a schema with a table named `locations`. Its has a key `observation_id_in_locations_table` which is a reference and we can join this to the primary table `observations` using the `observation_id` column.
      ```yml
        external_tables:
          locations:
            remote: observation_id_in_locations_table
            foreign: observation_id
      ```
      In many cases, the `foreign` and `remote` fields will be the same if you want to join the same key name from the primary table to the foreign table. (i.e. if you have a `observation_id` field in the primary table and `observation_id` field in the table for locations that associates the two)

### Postgres

The configuration for Postgres EDR is as follows:

```yaml
- type: edr
  name: pgedr.PostgresEDRProvider
  data: # Same as PostgresSQLProvider
    host: ${POSTGRES_HOST}
    dbname: ${POSTGRES_DB}
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
    # the schema in which the relevant tables are located
    search_path: [capture]
  table: waterservices_daily

  edr_fields: # Required EDR Fields
    id_field: id # Result identifier field
    geom_field: geometry # Result geometry field
    time_field: time # Result time field
    location_field: monitoring_location_id # Result location identifier field
    result_field: value # Result value/timeseries field
    parameter_id: parameter_code # Result parameter id field
    parameter_name: waterservices_timeseries_metadata.parameter_name # Result parameter name field
    parameter_unit: unit_of_measure # Result parameter unit field

  external_tables: # Additional table joins
    waterservices_timeseries_metadata: # JOIN waterservices_timeseries_metadata ON waterservices_daily.parameter_code=waterservices_timeseries_metadata.parameter_code
      foreign: parameter_code
      remote: parameter_code
```

### MySQL

The configuration for MySQL EDR is as follows:

```yaml
- type: edr
  name: pgedr.MySQLEDRProvider
  data: # Same as MySQLProvider
    host: ${MYSQL_HOST}
    port: ${MYSQL_PORT}
    dbname: ${MYSQL_DATABASE}
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
    search_path: [${MYSQL_DATABASE}]
  table: landing_observations

  edr_fields: # Required EDR Fields
    id_field: id
    geom_field: airports.airport_locations.geometry_wkt
    time_field: time
    location_field: location_id
    result_field: value
    parameter_id: parameter_id
    parameter_name: airport_parameters.name
    parameter_unit: airport_parameters.units

  external_tables: # Additional table joins
    airports: # JOIN airports ON landing_observations.location_id=airports.code
      foreign: location_id
      remote: code
    airports.airport_locations: # JOIN airport_locations ON airports.code=airport_locations.id
      foreign: code
      remote: id
    airport_parameters: # JOIN airport_parameters ON landing_observations.parameter_id=airport_parameters.id
      foreign: parameter_id
      remote: id

```
