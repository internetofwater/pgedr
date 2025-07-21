# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import functools
from typing import Any

from sqlalchemy import MetaData, PrimaryKeyConstraint
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import Select

from pygeoapi.provider.base import ProviderConnectionError

import logging

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


def recursive_getattr(obj: Any, attr: str) -> Any:
    """
    Recursively traverse an object's attributes single dot
    notation and return the final node.
    """
    for part in attr.split("."):
        obj = getattr(obj, part)
    return obj


def get_column_from_qualified_name(model: Any, path: str) -> Any:
    """
    Resolves a dot-qualified SQLAlchemy attribute path to the actual column.
    Supports relationships and backrefs.
    """
    # Check if there are more tables to hop
    parts = path.split(".", 1)

    try:
        attr = getattr(model, parts[0])
    except AttributeError:
        return model

    if len(parts) == 1:
        return attr  # Final attribute — expected to be a column

    # Follow the relationship
    related_model = attr.mapper.class_
    return get_column_from_qualified_name(related_model, parts[1])


@functools.cache
def get_base_schema(
    tables: tuple[str], schema: str, engine, id_column: str = None
):
    """Function used when automapping classes and relationships from
    database schema.
    """
    metadata = MetaData()

    try:
        # Reflect available tables first
        metadata.reflect(
            bind=engine,
            schema=schema,
            only=tables,
            views=True,
        )
    except OperationalError:
        raise ProviderConnectionError(
            f"Could not connect to {repr(engine.url)} (password hidden)."
        )

    # Add primary keys to views before automap
    for table_name, table in metadata.tables.items():
        if not table.primary_key.columns and table.columns:
            # Use the specified ID column if provided
            if id_column and id_column in table.columns:
                pk_col = table.columns[id_column]
            else:
                # Fallback to first column if ID column not found
                pk_col = list(table.columns)[0]
                LOGGER.warning(
                    f"ID column '{id_column}' not found in {table_name}, using {pk_col.name}"  # noqa: E501
                )

            pk_col.primary_key = True
            pk_constraint = PrimaryKeyConstraint(pk_col.name)
            table.append_constraint(pk_constraint)

            LOGGER.debug(f"Added primary key to {table_name}: {pk_col.name}")

    _Base = automap_base(metadata=metadata)
    _Base.prepare(
        name_for_scalar_relationship=_name_for_scalar_relationship,
    )
    return _Base


def _name_for_scalar_relationship(
    base, local_cls, referred_cls, constraint
) -> str:
    """Function used when automapping classes and relationships from
    database schema and fixes potential naming conflicts.
    """
    name = referred_cls.__name__.lower()
    local_table = local_cls.__table__
    if name in local_table.columns:
        newname = name + "_"
        LOGGER.debug(
            f"Already detected column name {name!r} in table "
            f"{local_table!r}. Using {newname!r} for relationship name."
        )
        return newname
    return name


def with_joins(query, joins, **kw):
    """Function to chain sql joins"""
    for join in joins:
        query = query.join(*join, **kw)
    return query


Select.with_joins = with_joins
