# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

FROM geopython/pygeoapi:latest

COPY *.toml *.yml /pgedr/
COPY src/ /pgedr/src/

RUN /venv/bin/python3 -m pip install -e /pgedr

ENV PYGEOAPI_CONFIG=/pgedr/pygeoapi.config.yml
ENV PYGEOAPI_OPENAPI=/pgedr/pygeoapi.openapi.yml

COPY docker/entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
