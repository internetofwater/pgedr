# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging
from typing import Any

from geoalchemy2.shape import to_shape
from shapely.geometry import shape, mapping

LOGGER = logging.getLogger(__name__)

GEOGRAPHIC_CRS = {
    'coordinates': ['x', 'y'],
    'system': {
        'type': 'GeographicCRS',
        'id': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
    },
}

TEMPORAL_RS = {
    'coordinates': ['t'],
    'system': {'type': 'TemporalRS', 'calendar': 'Gregorian'},
}


def empty_coverage():
    """
    Return empty Coverage dictionary.
    """
    return {
        'type': 'Coverage',
        'domain': {
            'type': 'Domain',
            'domainType': '',
            'axes': {'t': {'values': []}},
            'referencing': [GEOGRAPHIC_CRS, TEMPORAL_RS],
        },
        'ranges': {},
    }


def empty_coverage_collection():
    """
    Return empty Coverage Collection dictionary.
    """
    return {
        'type': 'CoverageCollection',
        'parameters': [],
        'coverages': [],
        'referencing': [GEOGRAPHIC_CRS, TEMPORAL_RS]
    }


def empty_range():
    """
    Return empty Range dictionary.
    """
    return {
        'type': 'NdArray',
        'dataType': 'float',
        'axisNames': ['t'],
        'shape': [0],
        'values': [],
    }


def read_geom(geom: Any, as_geojson: bool = False) -> Any:
    """
    Convert a geometry object to a Shapely geometry or GeoJSON-like dict.

    Tries to use geoalchemy2's to_shape first; if that fails,
    falls back to shapely's shape function.

    :param geom: Geometry object (GeoAlchemy2 or GeoJSON-like dict)
    :param as_geojson: If True, return geometry as GeoJSON-like dict.

    :return: Shapely geometry object or GeoJSON-like dict
    """
    try:
        geom = to_shape(geom)
    except Exception:
        geom = shape(geom)

    return mapping(geom) if as_geojson else geom


def apply_domain_geometry(domain: dict[str, Any], geom: Any) -> None:
    """
    Update a domain dictionary with geometry information.

    Sets the domainType and axes values based on the geometry type.
    For Point geometries, updates 'x' and 'y' axes.
    For other geometries, adds a 'composite' axis with coordinates.

    :param domain: Domain dictionary to update
    :param geom: Shapely geometry object
    """
    geom = read_geom(geom)
    domain['domainType'] = geom.geom_type.lstrip('Multi')
    if geom.geom_type == 'Point':
        domain['axes'].update(
            {'x': {'values': [geom.x]}, 'y': {'values': [geom.y]}}
        )
    else:
        values = mapping(geom)['coordinates']
        values = values if 'Multi' in geom.geom_type else [values]
        domain['axes']['composite'] = {
            'dataType': 'polygon',
            'coordinates': ['x', 'y'],
            'values': values,
        }
