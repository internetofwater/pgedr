# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging

from copy import deepcopy
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import functools
from typing import Optional, Any

from sqlalchemy import select, func, exists
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import (
    Session,
    aliased,
)
from sqlalchemy.sql.expression import or_, and_

from pygeoapi.crs import get_transform_from_spec, get_srid
from pygeoapi.provider.base import (
    ProviderItemNotFoundError,
    ProviderNoDataError,
)
from pygeoapi.provider.base_edr import BaseEDRProvider
from pygeoapi.provider.sql import (
    GenericSQLProvider,
    store_db_parameters,
    get_engine,
)

from pgedr.lib import (
    read_geom,
    empty_coverage,
    empty_range,
    apply_domain_geometry,
)

LOGGER = logging.getLogger(__name__)


LOCATION_TYPE_IDS = [
    1,
    2,
    3,
    4,
    9,
    10,
    12,
    15,
    16,
    17,
    22,
    23,
    25,
    27,
    28,
    29,
    25,
    35,
    # Exclude 38 (Worldwide)
    39,
    40,
    41,
]


class RISEEDRProvider(BaseEDRProvider):
    # Type hints for class attributes - set in __init__.store_db_parameters
    default_port = 3306
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_search_path: Optional[str]
    _db_password: str
    db_conn: str
    db_options: dict
    # Type hints for SQLAlchemy models - dynamically created by automap
    Location: Any
    Parameter: Any
    ParameterUnit: Any
    Results: Any

    store_db_parameters = store_db_parameters

    def __init__(
        self,
        provider_def: dict,
    ):
        """
        Initialize RISE EDR provider.

        :param provider_def: The provider definition from the pygeoapi config.

        :returns: pgedr.rise.RISEEDRProvider
        """
        LOGGER.debug('Initializing RISE EDR provider')

        BaseEDRProvider.__init__(self, provider_def)

        options = {'charset': 'utf8mb4'} | provider_def.get('options', {})
        self.store_db_parameters(provider_def['data'], options)
        self._engine = get_engine(
            'mysql+pymysql',
            self.db_host,
            self.db_port,
            self.db_name,
            self.db_user,
            self._db_password,
            self.db_conn,
            **self.db_options,
        )
        [
            self.Location,
            self.Parameter,
            self.ParameterUnit,
            self.Results,
            self.Item,
        ] = get_models(self._engine)

        # Determine `/locations` requires record present in results
        self.join_locations: bool = provider_def.get('join_locations', False)
        # Whether to sort results by dateTime in descending order
        self.sort_results: bool = provider_def.get('sort_results', False)
        # StatusID of 1 indicates active records in the database
        self.active_status_id: bool = provider_def.get(
            'active_status_id', True
        )
        # Whether to exclude modeled results (isModeled=1) from queries
        self.force_ignore_modeled: bool = provider_def.get(
            'force_ignore_modeled', True
        )

        self.get_fields()
        LOGGER.debug('Initialized RISE EDR provider')

    def get_fields(self):
        """
        Return fields (columns) from SQL table

        :returns: dict of fields
        """

        LOGGER.debug('Get available fields/properties')

        if not self._fields:
            LOGGER.debug('Querying database for fields/properties')
            with Session(self._engine) as session:
                query = select(
                    self.Parameter.parameterID,
                    self.Parameter.parameterName,
                    self.Parameter.parameterDescription,
                    self.ParameterUnit.parameterUnit,
                ).join(self.ParameterUnit)

                if self.active_status_id:
                    query = query.filter(self.Parameter.parameterStatusID == 1)

                query = query.distinct()

                result = self._compile_and_execute(session, query)
                for pid, pname, pdesc, punit in result:
                    self._fields[str(pid)] = {
                        'type': 'number',
                        'title': pname,
                        'description': pdesc,
                        'x-ogc-unit': punit,
                    }

        return self._fields

    def locations(
        self,
        location_id: Optional[str] = None,
        select_properties: list = [],
        bbox: list = [],
        datetime_: Optional[str] = None,
        limit: int = 100,
        **kwargs,
    ):
        """
        Extract and return locations from SQL table.

        :param location_id: Identifier of the location to filter by.
        :param select_properties: List of properties to include.
        :param bbox: Bounding box geometry for spatial queries.
        :param datetime_: Temporal filter for observations.
        :param limit: number of records to return (default 100)

        :returns: A GeoJSON or CovJSON of location data.
        """
        if location_id:
            LOGGER.debug(f'Filtering by location_id: {location_id}')
            return self.location(
                location_id=location_id,
                limit=limit,
                select_properties=select_properties,
                datetime_=datetime_,
            )

        LOGGER.debug('Preparing response')
        response = {
            'type': 'FeatureCollection',
            'features': [],
            'numberReturned': 0,
        }

        bbox_filter = self._get_bbox_filter(bbox)
        time_filter = self._get_datetime_filter(datetime_)
        parameter_filters = self._get_parameter_filters(select_properties)

        query = select(
            self.Location.locationID,
            self.Location.locationName,
            self.Location.locationCoordinates,
        ).filter(self.Location.locationTypeID != 38)

        if self.active_status_id:
            query = query.filter(self.Location.locationStatusID == 1)

        # Only apply joins if there are filters to apply
        join_conditions = [
            self.join_locations is True,
            time_filter is not True,
            parameter_filters is not True,
        ]
        if any(join_conditions):
            query = query.where(
                exists()
                .where(self.Results.locationID == self.Location.locationID)
                .where(time_filter)
                .where(parameter_filters)
            )

        if bbox_filter is not True:
            query = query.filter(bbox_filter)

        query = query.distinct().limit(limit)

        LOGGER.debug('Executing query to retrieve locations')
        with Session(self._engine) as session:
            for id, name, geom in self._compile_and_execute(session, query):
                response['features'].append(
                    {
                        'type': 'Feature',
                        'id': id,
                        'properties': {'name': name},
                        'geometry': read_geom(geom, as_geojson=True),
                    }
                )
                response['numberReturned'] += 1

        return response

    def location(
        self,
        location_id: str,
        limit: int = 100,
        select_properties: Optional[list] = [],
        datetime_: Optional[str] = None,
        **kwargs,
    ):
        """
        Extract and return single location from SQL table.

        :param location_id: Identifier of the location to filter by.
        :param limit: number of records to return (default 100)
        :param select_properties: List of properties to include.
        :param datetime_: Temporal filter for observations.

        :returns: A CovJSON of location data.
        """
        query = select(self.Location.locationCoordinates).filter(
            self.Location.locationID == location_id
        )

        if self.active_status_id:
            query = query.filter(self.Location.locationStatusID == 1)

        parameter_query = (
            select(self.Results.parameterID)
            .filter(self.Results.locationID == location_id)
            .where(
                exists()
                .where(self.Results.itemID == self.Item.itemID)
                .where(self.Item.itemRecordStatusID == 1)
                .where(self.Item.isModeled == 0)
            )
        )

        if select_properties:
            parameter_filters = self._get_parameter_filters(select_properties)
            parameter_query = parameter_query.filter(parameter_filters)

        with Session(self._engine) as session:
            geom = self._compile_and_execute(session, query).scalar()
            if not geom:
                msg = f'Location not found: {location_id}'
                raise ProviderItemNotFoundError(msg)

            select_parameters = {
                str(pid)
                for (pid,) in self._compile_and_execute(
                    session, parameter_query.distinct()
                )
            }
            if len(select_parameters) == 0:
                msg = f'Location has no data found: {location_id}'
                raise ProviderNoDataError(msg)

        coverage = empty_coverage(id=location_id)
        domain = coverage['domain']
        t_values: list = domain['axes']['t']['values']
        ranges = {}

        parameter_filters = self._get_parameter_filters(select_properties)
        time_filter = self._get_datetime_filter(datetime_)

        results = (
            select(self.Results.dateTime)
            .filter(self.Results.locationID == location_id)
            .filter(parameter_filters)
            .filter(time_filter)
        )

        if self.force_ignore_modeled:
            results = results.filter(
                exists()
                .where(self.Results.itemID == self.Item.itemID)
                .where(self.Item.itemRecordStatusID == 1)
                .where(self.Item.isModeled == 0)
            )

        if self.sort_results:
            results = results.order_by(self.Results.dateTime.desc())

        results = results.limit(limit)

        for parameter in select_parameters:
            ranges[parameter] = empty_range()
            results = self._construct_parameter_query(
                results, parameter, location_id
            )
        with Session(self._engine) as session:
            # Construct the query
            parameter_names = set()
            for row in self._compile_and_execute(session, results):
                row = row._asdict()

                # Add time value to domain
                t_values.append(row.pop('dateTime'))

                # Add parameter values to ranges
                for pname, value in row.items():
                    if value is not None:
                        parameter_names.add(pname)

                    ranges[pname]['values'].append(value)
                    ranges[pname]['shape'][0] += 1

            apply_domain_geometry(domain, geom)
            if len(t_values) > 1:
                domain['domainType'] += 'Series'

            coverage['parameters'] = self._get_parameters(parameter_names)
            coverage['ranges'] = {
                k: ranges[k] for k in ranges if k in parameter_names
            }

        if len(t_values) == 0:
            msg = f'Location has no data found: {location_id}'
            raise ProviderNoDataError(msg)

        return coverage

    def _get_parameters(self, parameters: set, aslist=False):
        """
        Generate parameters

        :param parameters: The datastream data to generate parameters for.
        :param aslist: The label for the parameter.

        :returns: A dictionary containing the parameter definition.
        """
        if not parameters:
            parameters = set(self.fields)

        out_params = {}
        for param in set(parameters):
            conf_ = self.fields[param]
            out_params[param] = {
                'id': param,
                'type': 'Parameter',
                'name': conf_['title'],
                'observedProperty': {
                    'id': param,
                    'label': {'en': conf_['title']},
                },
                'unit': {
                    'label': {'en': conf_['title']},
                    'symbol': {
                        'value': conf_['x-ogc-unit'],
                        'type': 'http://www.opengis.net/def/uom/UCUM/',
                    },
                },
            }

        return list(out_params.values()) if aslist else out_params

    def _get_parameter_filters(self, parameters: Optional[list]) -> Any:
        """
        Generate parameter filters

        :param parameters: The datastream data to generate filters for.

        :returns: A SQL alchemy filter for the parameters.
        """
        if not parameters:
            return True  # Let everything through

        # Convert parameter filters into SQL Alchemy filters
        filter_group = [
            self.Results.parameterID == str(value) for value in parameters
        ]
        return or_(*filter_group)

    def _get_datetime_filter(self, datetime_: Optional[str]) -> Any:
        if datetime_ in (None, '../..'):
            return True
        else:
            if '/' in datetime_:  # envelope
                LOGGER.debug('detected time range')
                time_begin, time_end = datetime_.split('/')
                if time_begin == '..':
                    datetime_filter = self.Results.dateTime <= time_end
                elif time_end == '..':
                    datetime_filter = self.Results.dateTime >= time_begin
                else:
                    datetime_filter = self.Results.dateTime.between(
                        time_begin, time_end
                    )
            else:
                datetime_filter = self.Results.dateTime == datetime_

        return datetime_filter

    def _get_bbox_filter(self, bbox: list[float]):
        """
        Construct the bounding box filter function
        """
        if not bbox:
            return True  # Let everything through if no bbox

        # If we are using mysql we can't use ST_MakeEnvelope since it is
        # postgis specific and thus we have to use MBRContains with a WKT
        # POLYGON

        # Create WKT POLYGON from bbox: (minx, miny, maxx, maxy)
        miny, minx, maxy, maxx = bbox
        polygon_wkt = f'POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))'  # noqa
        # Use MySQL MBRContains for index-accelerated bounding box checks
        storage_srid = get_srid(self.storage_crs)
        bbox_filter = func.MBRContains(
            func.ST_GeomFromText(polygon_wkt, storage_srid),
            func.ST_GeomFromGeoJSON(self.Location.locationCoordinates),
        )
        return bbox_filter

    def _construct_parameter_query(
        self, query: Any, parameter: str, location_id: str
    ):
        """
        Construct a query for a specific parameter and join to main query.

        :param query: The main SQLAlchemy query to join to.
        :param parameter: The parameter ID to filter by.
        :param location_id: The location ID to filter by.

        :returns: The modified SQLAlchemy query with the parameter joined in.
        """
        parameter_query = (
            select(
                self.Results.result,
                self.Results.dateTime,
            )
            .filter(self.Results.locationID == location_id)
            .filter(self.Results.parameterID == parameter)
        )

        if self.force_ignore_modeled:
            parameter_query = parameter_query.filter(
                exists()
                .where(self.Results.itemID == self.Item.itemID)
                .where(self.Item.itemRecordStatusID == 1)
                .where(self.Item.isModeled == 0)
            )

        model = aliased(self.Results, parameter_query.subquery())
        parameter_column = model.result.label(parameter)
        return query.join(
            model, self.Results.dateTime == model.dateTime
        ).add_columns(parameter_column)

    def _compile_and_execute(self, session, query):
        """
        Compile a SQLAlchemy query to a string with parameters rendered.

        :param query: The SQLAlchemy query to compile.

        :returns: A string of the compiled SQL query.
        """
        compiled_query = query.compile(
            compile_kwargs={'literal_binds': True},
            dialect=self._engine.dialect,
        )
        LOGGER.debug(f'Compiled query: {compiled_query}')
        return session.execute(query)


class RISEFeatureProvider(GenericSQLProvider):
    default_port = 3306

    def __init__(self, provider_def: dict):
        """
        MySQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor
        :returns: pgedr.rise.RISEFeatureProvider
        """

        driver_name = 'mysql+pymysql'
        extra_conn_args = {'charset': 'utf8mb4'}
        super().__init__(provider_def, driver_name, extra_conn_args)
        self.where_clauses = provider_def.get(
            'where_clauses',
            {'locationStatusID': 1, 'locationTypeID': LOCATION_TYPE_IDS},
        )

    def get_fields(self):
        """
        Return fields (columns) from database table

        :returns: dict of fields
        """

        LOGGER.debug('Get available fields/properties')

        # sql-schema only allows these types, so we need to map from sqlalchemy
        # string, number, integer, object, array, boolean, null,
        # https://json-schema.org/understanding-json-schema/reference/type.html
        column_type_map = {
            bool: 'boolean',
            datetime: 'string',
            Decimal: 'number',
            dict: 'object',
            float: 'number',
            int: 'integer',
            str: 'string',
        }
        default_type = 'string'

        # https://json-schema.org/understanding-json-schema/reference/string#built-in-formats  # noqa
        column_format_map = {
            'date': 'date',
            'interval': 'duration',
            'time': 'time',
            'timestamp': 'date-time',
        }

        def _column_type_to_json_schema_type(column_type):
            try:
                python_type = column_type.python_type
            except NotImplementedError:
                LOGGER.warning(f'Unsupported column type {column_type}')
                return default_type
            else:
                try:
                    return column_type_map[python_type]
                except KeyError:
                    LOGGER.warning(f'Unsupported column type {column_type}')
                    return default_type

        def _column_format_to_json_schema_format(column_type):
            try:
                ct = str(column_type).lower()
                return column_format_map[ct]
            except KeyError:
                LOGGER.debug('No string format detected')
                return None

        if not self._fields:
            for column in self.table_model.__table__.columns:  # type: ignore
                LOGGER.debug(f'Testing {column.name}')
                if column.name == self.geom:
                    continue

                if self.properties and column.name not in self.properties:
                    LOGGER.debug(
                        f'Skipping column {column.name} not in properties list'
                    )
                    continue

                self._fields[str(column.name)] = {
                    'type': _column_type_to_json_schema_type(column.type),
                    'format': _column_format_to_json_schema_format(
                        column.type
                    ),
                }

        return self._fields

    def get(self, identifier, crs_transform_spec=None, **kwargs):
        """
        Query the provider for a specific
        feature id e.g: /collections/hotosm_bdi_waterways/items/13990765

        :param identifier: feature id
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """
        LOGGER.debug(f'Get item by ID: {identifier}')

        # Execute query within self-closing database Session context
        with Session(self._engine) as session:
            # Retrieve data from database as feature
            item = session.get(self.table_model, identifier)  # type: ignore
            try:
                assert item is not None
                # Ensure returned row has exact match
                feature_id = getattr(item, self.id_field)
                assert str(feature_id) == identifier
                # Ensure return row has active status
                if hasattr(item, 'locationStatusID'):
                    status_id = getattr(item, 'locationStatusID')
                    assert status_id == 1
            except AssertionError as e:
                LOGGER.debug(e, exc_info=True)
                msg = f'No such item: {self.id_field}={identifier}.'
                raise ProviderItemNotFoundError(msg)

        crs_out = get_transform_from_spec(crs_transform_spec)  # type: ignore
        feature = self._sqlalchemy_to_feature(item, crs_out)

        # Drop non-defined properties
        if self.properties:
            props = feature['properties']
            dropping_keys = deepcopy(props).keys()
            for item in dropping_keys:
                if item not in self.properties:
                    props.pop(item)

        return feature

    def query(
        self,
        offset=0,
        limit=10,
        resulttype='results',
        bbox=[],
        datetime_=None,
        properties=[],
        sortby=[],
        select_properties=[],
        skip_geometry=False,
        q=None,
        filterq=None,
        crs_transform_spec=None,
        **kwargs,
    ):
        """
        Query the provider for features with additional filtering applied
        """
        if self.where_clauses:
            for col, val in self.where_clauses.items():
                LOGGER.debug(f'Applying where clause: {col} = {val}')
                if isinstance(val, list):
                    properties.extend([col, str(v).strip()] for v in val)
                else:
                    properties.append([col, val])

        return super().query(
            offset=offset,
            limit=limit,
            resulttype=resulttype,
            bbox=bbox,
            datetime_=datetime_,
            properties=properties,
            sortby=sortby,
            select_properties=select_properties,
            skip_geometry=skip_geometry,
            q=q,
            filterq=filterq,
            crs_transform_spec=crs_transform_spec,
            **kwargs,
        )

    def _get_bbox_filter(self, bbox: list[float]):
        """
        Construct the bounding box filter function
        """
        if not bbox:
            return True  # Let everything through if no bbox

        # If we are using mysql we can't use ST_MakeEnvelope since it is
        # postgis specific and thus we have to use MBRContains with a WKT
        # POLYGON

        # Create WKT POLYGON from bbox: (minx, miny, maxx, maxy)
        miny, minx, maxy, maxx = bbox
        polygon_wkt = f'POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))'  # noqa
        geom_column = getattr(self.table_model, self.geom)
        # Use MySQL MBRContains for index-accelerated bounding box checks
        storage_srid = get_srid(self.storage_crs)
        bbox_filter = func.MBRContains(
            func.ST_GeomFromText(polygon_wkt, storage_srid),
            func.ST_GeomFromGeoJSON(geom_column),
        )
        return bbox_filter

    def _get_property_filters(self, properties):
        if not properties:
            return True  # Let everything through

        # Group values by column name
        grouped = defaultdict(list)
        for column_name, value in properties:
            grouped[column_name].append(value)

        or_groups = []

        for column_name, values in grouped.items():
            column = getattr(self.table_model, column_name)

            # OR all values for the same column
            column_filters = [column == value for value in values]
            or_groups.append(or_(*column_filters))

        # OR across different columns
        return and_(*or_groups)


@functools.cache
def get_models(engine: Any) -> tuple:
    """
    Get SQLAlchemy models for the RISE EDR database tables.

    - Location: Represents observation locations with geometry.
    - Parameter: Represents observed parameters and their metadata.
    - ParameterUnit: Represents units associated with parameters.
    - Results: Represents observation results.

    :param engine: SQLAlchemy engine connected to the RISE EDR database.

    :returns: Tuple of SQLAlchemy models
    """
    LOGGER.debug('Getting table models')
    Base = automap_base()
    Base.prepare(autoload_with=engine)

    Location = Base.classes.location
    Parameter = Base.classes.parameter
    ParameterUnit = Base.classes.parameterUnit
    Results = Base.classes.results
    Item = Base.classes.item

    return Location, Parameter, ParameterUnit, Results, Item
