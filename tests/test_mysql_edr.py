# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from sqlalchemy.orm import Session, InstrumentedAttribute
import datetime
import pytest

from pgedr import MySQLEDRProvider
from pgedr.sql.lib import get_column_from_qualified_name as gqname
from pgedr.sql.lib import recursive_getattr as rgetattr


@pytest.fixture(params=['tables', 'views'])
def config(request):
    pygeoapi_config = {
        'name': 'MySQLEDRProvider',
        'type': 'edr',
        'data': {
            'host': 'localhost',
            'port': '3306',
            'dbname': 'airports',
            'user': 'mysql',
            'password': 'changeMe',
            'search_path': ['airports'],
        },
        'table': 'landing_observations',
        'edr_fields': {
            'id_field': 'id',
            'geom_field': 'airports.airport_locations.geometry_wkt',
            'time_field': 'time',
            'location_field': 'location_id',
            'result_field': 'value',
            'parameter_id': 'parameter_id',
            'parameter_name': 'airport_parameters.name',
            'parameter_unit': 'airport_parameters.units',
        },
        'external_tables': {
            'airports': {
                'foreign': 'location_id',
                'remote': 'code',
            },
            'airports.airport_locations': {
                'foreign': 'code',
                'remote': 'id',
            },
            'airport_parameters': {
                'foreign': 'parameter_id',
                'remote': 'id',
            },
        },
    }

    if request.param == 'tables':
        return pygeoapi_config

    if request.param == 'views':
        pygeoapi_config['edr_fields']['parameter_id'] = 'airport_parameters.id'
        pygeoapi_config['edr_fields']['location_field'] = (
            'airports.airport_locations.id'
        )
        return pygeoapi_config


def test_external_table_relationships(config):
    p = MySQLEDRProvider(config)

    assert p.table in p.table_models
    assert len(p.table_models) == 4

    for table in p.external_tables:
        assert gqname(p.model, table)


def test_can_query_single_edr_cols(config):
    p = MySQLEDRProvider(config)
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
        datetime.datetime(2025, 4, 30, 0, 0),
        'landings',
        'Daily plane landings',
        'count',
        'DCA',
        89,
    ]
    with Session(p._engine) as session:
        result = session.query(p.model).first()
        for edr_name, edr_val in zip(edr_names, edr_vals):
            assert rgetattr(result, edr_name) == edr_val

    with Session(p._engine) as session:
        query = session.query(p.model)
        for j in p.joins:
            query = query.join(*j)

        for edr_attr, edr_val in zip(edr_attrs, edr_vals):
            result = query.with_entities(edr_attr).limit(1).scalar()
            assert result == edr_val


def test_fields(config):
    """Testing query for a valid JSON object with geometry"""
    p = MySQLEDRProvider(config)

    assert len(p.fields) == 2
    for k, v in p.fields.items():
        assert k in ['landings', 'crashes']
        assert [k_ in ['title', 'type', 'x-ogc-unit'] for k_ in v]

    selected_mappings = {
        'landings': {
            'type': 'number',
            'title': 'Daily plane landings',
            'x-ogc-unit': 'count',
        },
    }
    for k, v in selected_mappings.items():
        assert p.fields[k] == v


def test_locations(config):
    p = MySQLEDRProvider(config)

    locations = p.locations()

    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 4

    feature = locations['features'][0]
    assert feature['id'] == 'ADW'
    assert feature['properties']['parameters'] == ['crashes', 'landings']


def test_locations_with_prop(config):
    config['properties'] = [
        'airline',
    ]
    p = MySQLEDRProvider(config)

    locations = p.locations()
    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 4

    feature = locations['features'][0]
    assert feature['id'] == 'ADW'
    assert feature.get('properties')
    assert 'airline' in feature.get('properties')


def test_locations_limit(config):
    p = MySQLEDRProvider(config)

    locations = p.locations(limit=1)
    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 1

    locations = p.locations(limit=500)
    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 4

    locations = p.locations(limit=3)
    assert locations['type'] == 'FeatureCollection'
    assert len(locations['features']) == 3


def test_locations_bbox(config):
    p = MySQLEDRProvider(config)

    locations = p.locations(bbox=[-77, 38.8, -76.9, 39])
    assert len(locations['features']) == 0


def test_locations_select_param(config):
    p = MySQLEDRProvider(config)

    locations = p.locations()
    assert len(locations['features']) == 4
    assert len(locations['parameters']) == 2

    locations = p.locations(select_properties=['crashes'])
    assert len(locations['features']) == 3
    assert len(locations['parameters']) == 1

    locations = p.locations(select_properties=['teleportations'])
    assert len(locations['features']) == 0
    assert len(locations['parameters']) == 0


def test_get_location(config):
    p = MySQLEDRProvider(config)

    location = p.locations(location_id='DCA')
    assert [k in location for k in ['type', 'domain', 'parameters', 'ranges']]

    assert location['type'] == 'Coverage'

    domain = location['domain']
    assert domain['type'] == 'Domain'
    assert domain['domainType'] == 'PointSeries'

    assert domain['axes']['x']['values'] == [-77.0377]
    assert domain['axes']['y']['values'] == [38.8512]
    assert domain['axes']['t']['values'] == [
        datetime.datetime(2025, 5, 4, 0, 0),
        datetime.datetime(2025, 5, 3, 0, 0),
        datetime.datetime(2025, 5, 2, 0, 0),
        datetime.datetime(2025, 5, 1, 0, 0),
        datetime.datetime(2025, 4, 30, 0, 0),
    ]

    t_len = len(domain['axes']['t']['values'])
    assert t_len == 5
    assert t_len == len(set(domain['axes']['t']['values']))

    assert [k in location for k in ['type', 'domain', 'parameters', 'ranges']]

    for param in location['parameters']:
        assert param in location['ranges']

    for range in location['ranges'].values():
        assert range['axisNames'][0] in domain['axes']
        assert range['shape'][0] == t_len
        assert len(range['values']) == t_len
        assert range['values'] == [88, 87, 85, 90, 89]


def test_locations_time(config):
    p = MySQLEDRProvider(config)

    locations = p.locations(datetime_='2025-04-30')
    assert len(locations['features']) == 1
    assert len(locations['parameters']) == 1
