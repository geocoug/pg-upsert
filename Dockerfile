# ~~~~~~~~~~~
# Build stage
# ~~~~~~~~~~~
FROM python:3.11-slim as staging
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update -y && \
    pip install --no-cache-dir --upgrade pip==24.1.2

COPY ./requirements.txt .

RUN pip wheel --no-cache-dir --wheel-dir /app/wheels -r requirements.txt


# ~~~~~~~~~~~
# Build final
# ~~~~~~~~~~~
FROM python:3.11-slim

ENV HOME=/app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN mkdir -p $HOME

WORKDIR $HOME

RUN addgroup --system app && \
    adduser --system --group app && \
    apt-get update && \
    rm -rf /var/lib/apt/lists/*

COPY --from=staging /app/wheels /wheels

# hadolint ignore=DL3013
RUN pip install --no-cache-dir --upgrade pip==24.1.2 && \
    pip install --no-cache-dir /wheels/*

COPY . $HOME

RUN chown -R app:app $HOME

USER app

ENTRYPOINT [ "python", "pg_upsert/pg_upsert.py" ]
