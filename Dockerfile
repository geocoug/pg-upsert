FROM python:3.12-slim

ENV HOME=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_SYSTEM_PYTHON=1

WORKDIR $HOME

# hadolint ignore=DL3008
RUN apt-get update -y \
    && apt-get install --no-install-recommends -y wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY ./requirements.txt /tmp/requirements.txt

RUN /bin/uv pip install --no-cache -r /tmp/requirements.txt \
    && rm -rf /tmp/requirements.txt

COPY ./pg_upsert $HOME/pg_upsert

COPY ./pyproject.toml $HOME

RUN /bin/uv pip install --no-cache $HOME

RUN addgroup --system app \
    && adduser --system --group app \
    && chown -R app:app $HOME

USER app

ENTRYPOINT [ "pg_upsert" ]
