.. pg_upsert documentation master file, created by
   sphinx-quickstart on Thu Jul 18 11:16:22 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pg_upsert documentation
=======================

.. image:: https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml/badge.svg
   :target: https://pypi.org/project/pg_upsert/
   :alt: CI/CD Badge

.. image:: https://img.shields.io/pypi/v/pg_upsert.svg
    :target: https://pypi.org/project/pg_upsert/
    :alt: PyPI Latest Release Badge

.. image:: https://img.shields.io/pypi/dm/pg_upsert.svg?label=pypi%20downloads
    :target: https://pypi.org/project/pg_upsert/
    :alt: pg_upsert Downloads Per Month Badge

.. image:: https://img.shields.io/pypi/pyversions/pg_upsert.svg
    :target: https://pypi.org/project/pg_upsert/
    :alt: Python Version Support Badge

**pg_upsert** is a Python package that runs not-NULL, Primary Key, Foreign Key, and Check Constraint checks on PostgreSQL staging tables then updates and inserts (upsert) data from staging tables to base tables.

Looking for examples? Check out the `examples <examples.html>`_ page.


Installation
------------

You can install **pg_upsert** via pip from PyPI:

.. code-block:: bash

   python -m venv .venv \
   && source .venv/bin/activate \
   && pip install pg_upsert

There is also a Docker image available on the GitHub Container Registry:

.. code-block:: bash

   docker pull ghcr.io/geocoug/pg_upsert:latest


Module Contents
---------------

.. toctree::
   :maxdepth: 4

   pg_upsert
   examples

Usage
-----

Python
^^^^^^

Below is a simple example of how to use the `PgUpsert` class to upsert data from staging tables to base tables. For a more detailed example, check out the `examples <examples.html>`_ page.

.. code-block:: python

    import logging

    from pg_upsert import PgUpsert

    logger = logging.getLogger("pg_upsert")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    PgUpsert(
        host="localhost",
        port=5432,
        database="dev",
        user="username",
        tables=("genres", "books", "authors", "book_authors"),
        stg_schema="staging",
        base_schema="public",
        do_commit=True,
        upsert_method="upsert",
        interactive=False,
    ).run()


CLI
^^^

`pg_upsert` has a command-line interface that can be used to perform all the functionality of the `PgUpsert` class. The CLI can be accessed by running `pg_upsert` in the terminal.

.. code-block:: bash

    usage: pg_upsert [-h] [-q] [-d] [-l LOGFILE] [-e EXCLUDE_COLUMNS] [-n NULL_COLUMNS] [-c] [-i] [-m METHOD] HOST DATABASE USER STAGING_SCHEMA BASE_SCHEMA TABLE [TABLE ...]

    Update and insert (upsert) data from staging tables to base tables.

    positional arguments:
    HOST                  database host
    DATABASE              database name
    USER                  database user
    STAGING_SCHEMA        staging schema name
    BASE_SCHEMA           base schema name
    TABLE                 table name(s)

    options:
    -h, --help            show this help message and exit
    -q, --quiet           suppress all console output
    -d, --debug           display debug output
    -l LOGFILE, --log LOGFILE
                            write log to LOGFILE
    -e EXCLUDE_COLUMNS, --exclude EXCLUDE_COLUMNS
                            comma-separated list of columns to exclude from null checks
    -n NULL_COLUMNS, --null NULL_COLUMNS
                            comma-separated list of columns to exclude from null checks
    -c, --commit          commit changes to database
    -i, --interactive     display interactive GUI of important table information
    -m METHOD, --method METHOD
                            method to use for upsert


Docker
^^^^^^

There is a Docker image available on the GitHub Container Registry that can be used to run `pg_upsert`:
.. code-block:: bash

    docker pull ghcr.io/geocoug/pg_upsert:latest


Credits
-------

This project was created using inspiration from `execsql <https://execsql.readthedocs.io/en/latest/index.html>`_ and the example script `pg_upsert.sql <https://osdn.net/projects/execsql-upsert/>`_. The goal of this project is to provide a Python implementation of `pg_upsert.sql` without the need for ExecSQL.
