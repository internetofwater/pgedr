# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import pytest

from pygeoapi.provider.base import (
    ProviderItemNotFoundError,
    ProviderNoDataError,
)
from pgedr.sql.rise import RISEEDRProvider


@pytest.fixture()
def config():
    return {
        'name': 'EDRProvider',
        'type': 'edr',
        'data': {
            'host': 'localhost',
            'port': '3307',
            'dbname': 'airports',
            'user': 'mysql',
            'password': 'changeMe',
        },
    }


def test_get_fields(config):
    p = RISEEDRProvider(config)

    assert len(p.fields) == 1574


def test_get_locations(config):
    p = RISEEDRProvider(config)

    response = p.locations()

    assert response['numberReturned'] == 5
    assert len(response['features']) == 5

    response = p.locations(limit=2)
    assert response['numberReturned'] == 2
    assert len(response['features']) == 2

    response = p.locations(limit=1000)
    assert response['numberReturned'] == 5
    assert len(response['features']) == 5


def test_get_locations_with_joining_locations(config):
    config['join_locations'] = True
    p = RISEEDRProvider(config)

    response = p.locations()

    assert response['numberReturned'] == 100
    assert len(response['features']) == 100

    response = p.locations(limit=10)
    assert response['numberReturned'] == 10
    assert len(response['features']) == 10

    response = p.locations(limit=1000)
    assert response['numberReturned'] == 646
    assert len(response['features']) == 646


def test_get_locations_with_param(config):
    p = RISEEDRProvider(config)

    response = p.locations(select_properties=[50])

    assert response['numberReturned'] == 1
    assert len(response['features']) == 1

    response = p.locations(select_properties=[50, 3])

    assert response['numberReturned'] == 4
    assert len(response['features']) == 4

    response = p.locations(select_properties=['not_a_real_parameter'])

    assert response['numberReturned'] == 0
    assert len(response['features']) == 0


def test_get_locations_with_datetime(config):
    p = RISEEDRProvider(config)

    response = p.locations(datetime_='2020-01-01')

    assert response['numberReturned'] == 0
    assert len(response['features']) == 0

    response = p.locations(datetime_='2020-01-01T00:00:00Z/..')

    assert response['numberReturned'] == 4
    assert len(response['features']) == 4

    response = p.locations(datetime_='../2020-01-01T00:00:00Z')

    assert response['numberReturned'] == 5
    assert len(response['features']) == 5


def test_get_location(config):
    p = RISEEDRProvider(config)

    with pytest.raises(ProviderItemNotFoundError):
        # Invalid location ID should raise error
        response = p.location(location_id='none')

    with pytest.raises(ProviderNoDataError):
        # Valid location ID but no data should raise error
        response = p.location(location_id='1')

    response = p.location(location_id='501')
    assert len(response['domain']['axes']['t']['values']) == 100
    assert len(response['ranges']['18']['values']) == 100
    assert len(response['ranges']) == 3


def test_get_location_with_sorted_results(config):
    p = RISEEDRProvider(config)

    response = p.location(location_id='501')
    t_values = response['domain']['axes']['t']['values']
    assert t_values != sorted(t_values, reverse=True)

    config['sort_results'] = True
    p = RISEEDRProvider(config)

    response = p.location(location_id='501')
    t_values = response['domain']['axes']['t']['values']
    assert t_values == sorted(t_values, reverse=True)


def test_get_location_with_limit(config):
    p = RISEEDRProvider(config)

    response = p.location(location_id='501', limit=10)
    assert len(response['domain']['axes']['t']['values']) == 10
    assert len(response['ranges']['18']['values']) == 10
    assert len(response['ranges']) == 3


def test_get_location_with_param(config):
    p = RISEEDRProvider(config)

    with pytest.raises(ProviderNoDataError):
        # Valid location ID but no data for parameter should raise error
        response = p.location(
            location_id='501', select_properties=['not_a_real_parameter']
        )

    response = p.location(location_id='501', select_properties=[18])
    assert len(response['domain']['axes']['t']['values']) == 100
    assert len(response['ranges']['18']['values']) == 100
    assert len(response['ranges']) == 1


def test_get_location_with_datetime(config):
    p = RISEEDRProvider(config)

    with pytest.raises(ProviderNoDataError):
        # Valid location ID but no data for datetime should raise error
        response = p.location(location_id='501', datetime_='2020-01-01')

    response = p.location(
        location_id='501', datetime_='2020-01-01T00:00:00Z/..'
    )
    assert len(response['domain']['axes']['t']['values']) == 100
    assert len(response['ranges']['18']['values']) == 100
    assert len(response['ranges']) == 3

    response = p.location(
        location_id='501',
        datetime_='2020-01-01T00:00:00Z/2020-02-01T00:00:00Z',
    )
    assert len(response['domain']['axes']['t']['values']) == 31
    assert len(response['ranges']['18']['values']) == 31
    assert len(response['ranges']) == 3
