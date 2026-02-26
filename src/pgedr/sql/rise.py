# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging

import functools
from geoalchemy2 import Geometry  # noqa - this isn't used explicitly but is needed to process Geometry columns
from typing import Optional, Any

from sqlalchemy import select
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import (
    Session,
    aliased,
)
from sqlalchemy.sql.expression import or_

from pygeoapi.provider.base import (
    ProviderItemNotFoundError,
    ProviderNoDataError,
)
from pygeoapi.provider.base_edr import BaseEDRProvider
from pygeoapi.provider.sql import store_db_parameters, get_engine

from pgedr.lib import (
    read_geom,
    empty_coverage,
    empty_range,
    apply_domain_geometry,
)

LOGGER = logging.getLogger(__name__)


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
        store_db_parameters(self, provider_def['data'], options)
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
        [self.Location, self.Parameter, self.ParameterUnit, self.Results] = (
            get_models(self._engine)
        )

        self.join_locations = provider_def.get('join_locations', False)
        self.sort_results = provider_def.get('sort_results', False)

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

                for pid, pname, pdesc, punit in session.execute(query):
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
        datetime_: Optional[str] = None,
        limit: int = 100,
        **kwargs,
    ):
        """
        Extract and return locations from SQL table.

        :param location_id: Identifier of the location to filter by.
        :param select_properties: List of properties to include.
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

        time_filter = self._get_datetime_filter(datetime_)
        parameter_filters = self._get_parameter_filters(select_properties)
        filters = [time_filter, parameter_filters]

        query = select(
            self.Location.locationID,
            self.Location.locationName,
            self.Location.locationCoordinates,
        )

        if not self.join_locations or filters != [True, True]:
            # Only apply joins if there are filters to apply
            query = query.join(
                self.Results,
                self.Results.locationID == self.Location.locationID,
            ).filter(*filters)

        query = query.distinct().limit(limit)

        LOGGER.debug('Executing query to retrieve locations')
        LOGGER.debug(f'Query: {self._compile_query(query)}')
        with Session(self._engine) as session:
            for id, name, geom in session.execute(query):
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
        query = (
            select(self.Location.locationCoordinates)
            .filter(self.Location.locationID == location_id)
            .limit(1)
        )
        parameter_query = (
            select(self.Results.parameterID)
            .filter(self.Results.locationID == location_id)
            .distinct()
        )
        with Session(self._engine) as session:
            geom = session.execute(query).scalar()
            if not geom:
                msg = f'Location not found: {location_id}'
                raise ProviderItemNotFoundError(msg)

            if select_properties:
                select_parameters = set(map(str, select_properties))
            else:
                select_parameters = set(
                    [str(pid) for (pid,) in session.execute(parameter_query)]
                )
                if len(select_parameters) == 0:
                    msg = f'Location has no data found: {location_id}'
                    raise ProviderNoDataError(msg)

        coverage = empty_coverage()
        coverage['id'] = location_id
        ranges = {}
        domain = coverage['domain']
        t_values: list = domain['axes']['t']['values']

        parameter_filters = self._get_parameter_filters(select_properties)
        time_filter = self._get_datetime_filter(datetime_)

        results = (
            select(self.Results.dateTime)
            .filter(self.Results.locationID == location_id)
            .filter(parameter_filters)
            .filter(time_filter)
            .distinct()
        )

        if self.sort_results:
            results = results.order_by(self.Results.dateTime.desc())

        results = results.limit(limit)

        LOGGER.error(f'select_parameters: {select_parameters}')
        for parameter in select_parameters:
            parameter_query = (
                select(
                    self.Results.result,
                    self.Results.dateTime,
                )
                .filter(time_filter)
                .filter(self.Results.locationID == location_id)
                .filter(self.Results.parameterID == parameter)
                .subquery()
            )
            model = aliased(self.Results, parameter_query)
            ranges[parameter] = empty_range()
            results = results.join(
                model, self.Results.dateTime == model.dateTime
            ).add_columns(model.result.label(parameter))

        with Session(self._engine) as session:
            # Construct the query
            parameter_names = set()
            for row in session.execute(results):
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

    def _sqlalchemy_to_feature(self, id: str, geom: Any, name: str) -> dict:
        """
        Create GeoJSON of location.

        :param id: Identifier of the location.
        :param geom: Geometry of the location.
        :param name: Additional fields for feature properties.

        :returns: A Feature of location data.
        """

        feature = {
            'type': 'Feature',
            'id': id,
            'properties': {'name': name},
            'geometry': read_geom(geom, as_geojson=True),
        }

        return feature

    def _get_parameters(self, parameters: set, aslist=False):
        """
        Generate parameters

        :param parameters: The datastream data to generate parameters for.
        :param aslist: The label for the parameter.

        :returns: A dictionary containing the parameter definition.
        """
        if not parameters:
            parameters = set(self.fields.keys())

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

    def _compile_query(self, query):
        """
        Compile a SQLAlchemy query to a string with parameters rendered.

        :param query: The SQLAlchemy query to compile.

        :returns: A string of the compiled SQL query.
        """
        compiled = query.compile(
            compile_kwargs={'literal_binds': True},
            dialect=self._engine.dialect,
        )
        return str(compiled)


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

    return Location, Parameter, ParameterUnit, Results
