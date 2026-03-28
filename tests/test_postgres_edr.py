# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from sqlalchemy.orm import Session, InstrumentedAttribute
import datetime
import pytest

from pygeoapi.provider.base import (
    ProviderInvalidDataError,
    ProviderItemNotFoundError,
)

from pgedr import PostgresEDRProvider
from pgedr.sql.lib import get_column_from_qualified_name as gqname
from pgedr.sql.lib import recursive_getattr as rgetattr


@pytest.fixture()
def config():
    pygeoapi_config = {
        'name': 'PostgresEDRProvider',
        'type': 'edr',
        'data': {
            'host': 'localhost',
            'dbname': 'edr',
            'user': 'postgres',
            'password': 'changeMe',
            'search_path': ['edr_quickstart'],
        },
        'table': 'observations',
        'properties': ['locations.properties'],
        'edr_fields': {
            'id_field': 'observation_id',
            'location_field': 'locations.location_id',
            'geom_field': 'locations.geometry',
            'time_field': 'observation_time',
            'result_field': 'observation_value',
            'parameter_id': 'parameters.parameter_id',
            'parameter_name': 'parameters.parameter_unit_label',
            'parameter_unit': 'parameters.parameter_unit_symbol',
        },
        'external_tables': {
            'locations': {
                'foreign': 'location_id',
                'remote': 'location_id',
            },
            'parameters': {
                'foreign': 'parameter_id',
                'remote': 'parameter_id',
            },
        },
    }
    return pygeoapi_config


def test_invalid_config(config):
    config['edr_fields']['parameter_id'] = 'invalid_parameter_id'
    with pytest.raises(ProviderInvalidDataError):
        PostgresEDRProvider(config)


def test_bad_location_id(config):
    p = PostgresEDRProvider(config)

    with pytest.raises(ProviderItemNotFoundError):
        p.locations(location_id=1465791)


def test_external_table_relationships(config):
    p = PostgresEDRProvider(config)

    assert p.table in p.table_models
    assert len(p.table_models) == 3

    for table in p.external_tables:
        assert hasattr(p.model, table)


def test_can_query_single_edr_cols(config):
    p = PostgresEDRProvider(config)
    edr_attrs = [p.tc, p.pic, p.pnc, p.puc, p.lc, p.rc]
    assert all([isinstance(f, InstrumentedAttribute) for f in edr_attrs])
    assert gqname(p.model, p.parameter_id) == p.pic

    edr_names = [
        p.time_field,
        p.parameter_id,
        p.parameter_name,
        p.parameter_unit,
        p.location_field,
        p.result_field,
    ]
    edr_vals = [
        datetime.datetime(2025, 9, 10, 9, 0, tzinfo=datetime.timezone.utc),
        'TEMP',
        'Celsius',
        '°C',
        1,
        22.5,
    ]
    with Session(p._engine) as session:
        result = session.query(p.model).first()
        for edr_name, edr_val in zip(edr_names, edr_vals):
            assert rgetattr(result, edr_name) == edr_val


def test_fields(config):
    """Testing query for a valid JSON object with geometry"""
    p = PostgresEDRProvider(config)

    assert len(p.fields) == 3
    for k, v in p.fields.items():
        assert [k_ in ['title', 'type', 'x-ogc-unit'] for k_ in v]

    selected_mappings = {
        'AQI': {
            'type': 'number',
            'title': 'Air Quality Index',
            'x-ogc-unit': 'AQI',
        },
        'TEMP': {
            'type': 'number',
            'title': 'Celsius',
            'x-ogc-unit': '°C',
        },
        'HUM': {
            'type': 'number',
            'title': 'Percent',
            'x-ogc-unit': '%',
        },
    }
    for k, v in selected_mappings.items():
        assert p.fields[k] == v


def test_locations(config):
    p = PostgresEDRProvider(config)

    locations = p.locations()

    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 3

    feature = locations['features'][0]
    assert feature['id'] == 1


def test_locations_limit(config):
    p = PostgresEDRProvider(config)

    locations = p.locations(limit=1)
    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 1

    locations = p.locations(limit=500)
    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 3

    locations = p.locations(limit=2)
    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 2


def test_locations_bbox(config):
    p = PostgresEDRProvider(config)

    locations = p.locations(bbox=[-74, 40, -73, 41])
    assert len(locations['features']) == 1


def test_cube(config):
    p = PostgresEDRProvider(config)

    response = p.cube(bbox=[-109, 34, -50, 45], limit=1)
    assert len(response['coverages']) == 2

    response = p.cube(
        bbox=[-109, 34, -50, 45], limit=1, select_properties=['TEMP']
    )
    assert len(response['coverages']) == 2


def test_area(config):
    p = PostgresEDRProvider(config)
    wkt = 'POLYGON((-109 34, -50 34, -50 45, -109 45, -109 34))'
    response = p.area(wkt=wkt, limit=1)
    assert len(response['coverages']) == 2

    response = p.area(wkt=wkt, limit=1, select_properties=['TEMP'])
    assert len(response['coverages']) == 2


def test_locations_select_param(config):
    p = PostgresEDRProvider(config)

    locations = p.locations()

    locations = p.locations(select_properties=['HUM'])
    assert len(locations['features']) == 2

    locations = p.locations(select_properties=['TEMP'])
    assert len(locations['features']) == 3

    locations = p.locations(select_properties=['TEMP', 'HUM'])
    assert len(locations['features']) == 3


def test_get_location(config):
    p = PostgresEDRProvider(config)

    location = p.locations(location_id=1)
    assert [k in location for k in ['type', 'domain', 'parameters', 'ranges']]

    assert location['type'] == 'Coverage'

    domain = location['domain']
    assert domain['type'] == 'Domain'
    assert domain['domainType'] == 'PointSeries'

    assert domain['axes']['x']['values'] == [-73.9654]
    assert domain['axes']['y']['values'] == [40.7829]
    assert domain['axes']['t']['values'] == [
        datetime.datetime(2025, 9, 10, 10, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2025, 9, 10, 9, 0, tzinfo=datetime.timezone.utc),
    ]

    t_len = len(domain['axes']['t']['values'])
    assert t_len == 2
    assert t_len == len(set(domain['axes']['t']['values']))

    assert [k in location for k in ['type', 'domain', 'parameters', 'ranges']]

    for param in location['parameters']:
        assert param in location['ranges']

    range = location['ranges']['TEMP']
    assert range['axisNames'][0] in domain['axes']
    assert range['shape'][0] == t_len
    assert len(range['values']) == t_len
    assert range['values'] == [21.5, 22.5]


def test_invalid_location(config):
    p = PostgresEDRProvider(config)

    with pytest.raises(ProviderItemNotFoundError):
        p.locations(location_id=1234)

    with pytest.raises(ProviderItemNotFoundError):
        p.locations(location_id='1234')


def test_expand_properties(config):
    p = PostgresEDRProvider(config)

    locations = p.locations(datetime_='2024-10-09T00:00:00Z/..')
    assert len(locations['features']) == 3


def test_jsonb_property_expansion(config):
    """Ensure 'properties' is expanded and there is not a duplicate
    nested properties key"""
    p = PostgresEDRProvider(config)

    locations = p.locations()
    for loc in locations['features']:
        assert 'properties' not in loc['properties']
        assert 'type' in loc['properties'], (
            'type should be directly in the properties field'
        )
