ARG UV_VERSION=0.10.9
ARG PYTHON_VERSION=3.11

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM amazonlinux:2023 AS build

ARG PYTHON_VERSION

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/build/environment \
    VIRTUAL_ENV=/build/environment \
    PATH=/build/environment/bin:/usr/local/bin:${PATH}

RUN dnf install -y \
        ca-certificates \
        gcc \
        gzip \
        tar \
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-devel \
    && dnf clean all \
    && rm -rf /var/cache/dnf

COPY --from=uv /uv /uvx /usr/local/bin/

WORKDIR /src

COPY pyproject.toml uv.lock ./

RUN mkdir -p /build /out \
    && python${PYTHON_VERSION} -m venv "${VIRTUAL_ENV}"

RUN uv sync --locked --no-dev --no-install-project

RUN venv-pack -o /out/environment.tar.gz

FROM scratch AS artifact

COPY --from=build /out/environment.tar.gz /environment.tar.gz
