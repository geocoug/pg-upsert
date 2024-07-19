import logging

from pg_upsert import PgUpsert

logger = logging.getLogger("pg_upsert")
logger.setLevel(logging.INFO)
handlers = [
    logging.FileHandler("pg_upsert.log"),
    logging.StreamHandler(),
]
if logger.level == logging.DEBUG:
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(lineno)d: %(message)s"
    )
else:
    formatter = logging.Formatter("%(message)s")
for handler in handlers:
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Full example of instantiating the class and running the upserts
upsert = PgUpsert(
    host="localhost",
    port=5432,
    database="dev",
    user="docker",
    passwd="docker",
    tables=("genres", "books", "authors", "book_authors"),
    stg_schema="staging",
    base_schema="public",
    do_commit=False,
    upsert_method="insert",
    interactive=False,
    exclude_cols=None,
    exclude_null_check_columns=None,
    control_table="ups_control",
).run()


# Minimal example of instantiating the class and running the upserts
# upsert = PgUpsert(
#     host="localhost",
#     port=5432,
#     database="dev",
#     user="docker",
#     passwd="docker",
#     tables=("genres", "books", "authors", "book_authors"),
#     stg_schema="staging",
#     base_schema="public",
# )

# upsert.run()

# # Modify the control table then run run upsert on the one table
# upsert.db.execute(
#     f"update {upsert.control_table} set exclude_cols = 'first_name,last_name', interactive=true where table_name = 'authors';"
# )
# upsert.upsert_one(table="authors").commit()


# # Run upsert on one table
# upsert.upsert_one(table="authors").commit()


# # Run a specific set of qa checks on one table
# # Null checks
# upsert.qa_one_null("authors")
# # Primary key checks
# upsert.qa_one_pk("authors")
# # Foreign key checks
# upsert.qa_one_fk("authors")
# # Check constraint checks
# upsert.qa_one_ck("authors")


# # Run everything with defaults
# upsert.run()


# # Run only QA checks
# upsert.qa_all()


# # Run only upserts and commit changes (if do_commit=True)
# upsert.upsert_all().commit()


# # Run only upserts and do not commit changes
# upsert.upsert_all()


# # Modify the control table on a table-by-table basis
# # The control table is initialized when the class is instantiated
# logger.info(upsert.show(f"select * from {upsert.control_table}"))
# logger.info("")
# # Modify the exclude_cols column for the authors table and set interactive to true. The exclude_cols and exclude_null_checks values should be a comma-separated string.
# upsert.db.execute(
#     f"update {upsert.control_table} set exclude_cols = 'first_name,last_name', interactive=true where table_name = 'authors';"
# )
# logger.info(upsert.show(f"select * from {upsert.control_table}"))


del upsert
