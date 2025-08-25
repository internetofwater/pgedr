# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

GEOGRAPHIC_CRS = {
    "coordinates": ["x", "y"],
    "system": {
        "type": "GeographicCRS",
        "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
    },
}

TEMPORAL_RS = {
    "coordinates": ["t"],
    "system": {"type": "TemporalRS", "calendar": "Gregorian"},
}


def empty_coverage() -> dict[str, Any]:
    """
    Return empty Coverage dictionary.
    """
    return {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "domainType": "",
            "axes": {"t": {"values": []}},
            "referencing": [GEOGRAPHIC_CRS, TEMPORAL_RS],
        },
        "ranges": {},
    }


def empty_coverage_collection() -> dict[str, Any]:
    """
    Return empty Coverage Collection dictionary.
    """
    return {
        "type": "CoverageCollection",
        "domainType": "Point",
        "referencing": [GEOGRAPHIC_CRS, TEMPORAL_RS],
        "parameters": [],
        "coverages": [],
    }


def empty_range() -> dict[str, Any]:
    """
    Return empty Range dictionary.
    """
    return {
        "type": "NdArray",
        "dataType": "float",
        "axisNames": ["t"],
        "shape": [0],
        "values": [],
    }
