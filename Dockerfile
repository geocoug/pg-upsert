FROM python:3.13-alpine AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_SYSTEM_PYTHON=1

WORKDIR /tmp/build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY ./pyproject.toml .
COPY ./src/pg_upsert ./pg_upsert

RUN /bin/uv pip install --no-cache /tmp/build

FROM python:3.13-alpine AS final

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apk add --no-cache tk ttf-dejavu fontconfig

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

RUN addgroup -S app \
    && adduser -S -G app app \
    && chown -R app:app /app

USER app

ENTRYPOINT [ "pg-upsert" ]
