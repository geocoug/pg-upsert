#!/usr/bin/env python
"""Tests for method-aware QA: checks only consider rows ``upsert_all()`` will write.

Both test schemas seed the base tables with existing rows (see "Seed the base
tables" in ``tests/data/schema_passing.sql`` and ``schema_failing.sql``), so
some staging rows are updates and others are inserts. In the passing schema:

- ``authors.JDoe`` exists with email ``jdoe.old@email.com``; staging moves it
  to ``john.doe@email.com``. ``authors.ZOld`` exists only in the base table.
- ``books.B001`` exists (genre ``Fiction``, publisher ``P001``).
- ``genres`` Fiction and Poetry exist; ``Western`` exists only in the base table.
- ``publishers.publisher_name`` has a bare unique index, ``uq_publishers_name``,
  and ``P900`` exists only in the base table.
"""

from __future__ import annotations

import pytest
from psycopg.sql import SQL

from pg_upsert.models import QACheckType
from pg_upsert.upsert import PgUpsert

pytestmark = pytest.mark.postgres

METHODS = ("upsert", "update", "insert")
TABLES = ("genres", "books", "authors", "book_authors", "publishers")


def make_ups(db, method: str = "upsert", exclude_cols=("rev_user", "rev_time"), tables=TABLES) -> PgUpsert:
    """Build a PgUpsert against the database *db* was loaded with."""
    return PgUpsert(
        conn=db.conn,
        tables=tables,
        staging_schema="staging",
        base_schema="public",
        do_commit=False,
        interactive=False,
        upsert_method=method,
        exclude_cols=exclude_cols,
    )


def effective(ups: PgUpsert, table: str, cols: str) -> list[tuple]:
    rows = ups.db.execute(
        SQL("select {cols} from {src} order by 1").format(
            cols=SQL(cols),
            src=ups._qa._effective_rows(table),
        ),
    ).fetchall()
    return [tuple(r) for r in rows]


def flagged(errors) -> bool:
    return any(e.severity.value == "error" for e in errors)


def error_summary(ups: PgUpsert) -> set[tuple[str, str]]:
    return {(e.table, e.check_type.value) for e in ups.qa_errors if e.severity.value == "error"}


def error_details(ups: PgUpsert, table: str, check: QACheckType) -> list[str]:
    return [e.details for e in ups.qa_errors if e.table == table and e.check_type == check]


# ===================================================================
# Whole-schema behavior per method
# ===================================================================


class TestPassingSchemaByMethod:
    # (rows_updated, rows_inserted) per table after upsert_all().
    EXPECTED_COUNTS = {
        "upsert": {
            "genres": (2, 17),
            "publishers": (1, 20),
            "books": (1, 17),
            "authors": (1, 12),
            "book_authors": (0, 18),
        },
        "update": {
            "genres": (2, 0),
            "publishers": (1, 0),
            "books": (1, 0),
            "authors": (1, 0),
            "book_authors": (0, 0),
        },
        "insert": {
            "genres": (0, 17),
            "publishers": (0, 20),
            "books": (0, 17),
            "authors": (0, 12),
            "book_authors": (0, 18),
        },
    }

    @pytest.mark.parametrize("method", METHODS)
    def test_qa_passes_and_load_succeeds(self, db, method):
        """QA passing must mean the load succeeds."""
        ups = make_ups(db, method).qa_all()
        assert ups.qa_passed is True
        ups.upsert_all()
        rows = db.execute("select table_name, rows_updated, rows_inserted from ups_control;").fetchall()
        counts = {table: (updated, inserted) for table, updated, inserted in rows}
        assert counts == self.EXPECTED_COUNTS[method]

    @pytest.mark.parametrize(
        ("method", "expected_email"),
        [("upsert", "john.doe@email.com"), ("update", "john.doe@email.com"), ("insert", "jdoe.old@email.com")],
    )
    def test_existing_row_updated_only_when_method_updates(self, db, method, expected_email):
        make_ups(db, method).qa_all().upsert_all()
        email = db.execute("select email from public.authors where author_id = 'JDoe';").fetchone()[0]
        assert email == expected_email


class TestFailingSchemaByMethod:
    UPSERT_AND_INSERT_ERRORS = {
        ("publishers", "type"),
        ("authors", "length"),
        ("genres", "null"),
        ("books", "null"),
        ("authors", "null"),
        ("book_authors", "null"),
        ("publishers", "null"),
        ("genres", "pk"),
        ("books", "pk"),
        ("authors", "pk"),
        ("book_authors", "pk"),
        ("publishers", "pk"),
        ("authors", "unique"),
        ("books", "fk"),
        ("book_authors", "fk"),
        ("authors", "ck"),
    }

    @pytest.mark.parametrize("method", ["upsert", "insert"])
    def test_upsert_and_insert_errors(self, db_failing, method):
        ups = make_ups(db_failing, method).qa_all()
        assert ups.qa_passed is False
        assert error_summary(ups) == self.UPSERT_AND_INSERT_ERRORS

    def test_update_only_checks_existing_rows(self, db_failing):
        """Only the staging rows whose PK is already in the base table are written."""
        ups = make_ups(db_failing, "update").qa_all()
        assert error_summary(ups) == {("publishers", "type"), ("authors", "pk"), ("books", "fk")}
        # The two staging JDoe rows both target the seeded base row.
        assert error_details(ups, "authors", QACheckType.PRIMARY_KEY) == [
            "1 duplicate keys (2 rows) in table staging.authors",
        ]
        # B001 moves to 'Mystery', which only exists in staging and is never inserted.
        assert error_details(ups, "books", QACheckType.FOREIGN_KEY) == ["books_genre_fkey (1)"]

    @pytest.mark.parametrize(
        ("method", "expected"),
        [
            ("upsert", "2 duplicate keys (4 rows) in table staging.authors"),
            ("insert", "1 duplicate keys (2 rows) in table staging.authors"),
        ],
    )
    def test_insert_ignores_duplicates_of_existing_rows(self, db_failing, method, expected):
        """JDoe already exists, so insert mode never writes its duplicate staging rows."""
        ups = make_ups(db_failing, method).qa_all()
        assert error_details(ups, "authors", QACheckType.PRIMARY_KEY) == [expected]

    @pytest.mark.parametrize(
        ("method", "expected"),
        [
            ("upsert", ["books_genre_fkey (1)", "books_publisher_id_fkey (1)"]),
            ("insert", ["books_genre_fkey (1)", "books_publisher_id_fkey (1)"]),
        ],
    )
    def test_book_fk_errors(self, db_failing, method, expected):
        """'Horrorr' and 'P999' are always orphans; B001's 'Mystery' is inserted alongside it."""
        ups = make_ups(db_failing, method).qa_all()
        assert sorted(error_details(ups, "books", QACheckType.FOREIGN_KEY)) == expected

    @pytest.mark.parametrize("method", ["upsert", "insert"])
    def test_unique_reports_base_collision(self, db_failing, method):
        """New staging author EEvans uses the email of base-only author ZOld."""
        ups = make_ups(db_failing, method).qa_all()
        assert error_details(ups, "authors", QACheckType.UNIQUE) == [
            "uq_authors_email (2 duplicates, 4 rows, 1 with existing base rows)",
        ]


# ===================================================================
# Effective / predicted rows
# ===================================================================


class TestEffectiveRows:
    @pytest.mark.parametrize(
        ("method", "expected_count", "jdoe_included"),
        [("upsert", 13, True), ("update", 1, True), ("insert", 12, False)],
    )
    def test_rows_follow_method(self, db, method, expected_count, jdoe_included):
        rows = effective(make_ups(db, method), "authors", "author_id")
        assert len(rows) == expected_count
        assert (("JDoe",) in rows) is jdoe_included

    def test_upsert_without_excludes_is_staging_table(self, db):
        src = make_ups(db, "upsert")._qa._effective_rows("authors").as_string(db.conn)
        assert src == '"staging"."authors" as "s"'

    def test_excluded_column_uses_base_value_on_update(self, db):
        ups = make_ups(db, "upsert", exclude_cols=("email",))
        rows = dict(effective(ups, "authors", "author_id, email"))
        assert rows["JDoe"] == "jdoe.old@email.com"

    def test_excluded_column_is_null_on_insert(self, db):
        ups = make_ups(db, "upsert", exclude_cols=("email",))
        rows = dict(effective(ups, "authors", "author_id, email"))
        assert rows["AAdams"] is None

    def test_columns_match_staging(self, db):
        ups = make_ups(db, "update", exclude_cols=("email",))
        cur = db.execute(SQL("select * from {src} limit 0").format(src=ups._qa._effective_rows("authors")))
        assert [c.name for c in cur.description] == ["author_id", "first_name", "last_name", "email", "fixed_code"]


class TestPredictedRows:
    @pytest.mark.parametrize(
        ("method", "expected_jdoe_email"),
        [
            ("upsert", "john.doe@email.com"),
            ("update", "john.doe@email.com"),
            ("insert", "jdoe.old@email.com"),
        ],
    )
    def test_base_row_replaced_only_when_updated(self, db, method, expected_jdoe_email):
        rows = db.execute(
            SQL("select author_id, email, _ups_src from {src} where author_id = 'JDoe'").format(
                src=make_ups(db, method)._qa._predicted_rows("authors", ["email"]),
            ),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == expected_jdoe_email

    @pytest.mark.parametrize("method", METHODS)
    def test_untouched_base_rows_are_kept(self, db, method):
        rows = db.execute(
            SQL("select author_id, _ups_src from {src}").format(
                src=make_ups(db, method)._qa._predicted_rows("authors", ["email"]),
            ),
        ).fetchall()
        assert ("ZOld", "base") in [tuple(r) for r in rows]


# ===================================================================
# Row-local checks
# ===================================================================


class TestNullCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_null_on_new_row(self, db, method, expected):
        db.execute("update staging.authors set first_name = null where author_id = 'AAdams';")
        assert flagged(make_ups(db, method)._qa.check_nulls("authors")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", True), ("insert", False)])
    def test_null_on_existing_row(self, db, method, expected):
        db.execute("update staging.authors set first_name = null where author_id = 'JDoe';")
        assert flagged(make_ups(db, method)._qa.check_nulls("authors")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_excluded_required_column_is_null_on_insert(self, db, method, expected):
        """An excluded NOT NULL column without a default is NULL in every inserted row."""
        ups = make_ups(db, method, exclude_cols=("first_name",))
        assert flagged(ups._qa.check_nulls("authors")) is expected


class TestLengthCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", True), ("insert", False)])
    def test_overflow_on_existing_row(self, db, method, expected):
        db.execute("update staging.authors set email = repeat('x', 101) where author_id = 'JDoe';")
        assert flagged(make_ups(db, method)._qa.check_lengths("authors")) is expected


class TestPrimaryKeyCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_duplicate_new_key(self, db, method, expected):
        db.execute("insert into staging.authors (author_id, first_name, last_name) values ('AAdams', 'Al', 'Adams');")
        assert flagged(make_ups(db, method)._qa.check_pks("authors")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", True), ("insert", False)])
    def test_duplicate_existing_key(self, db, method, expected):
        db.execute("insert into staging.authors (author_id, first_name, last_name) values ('JDoe', 'Jon', 'Doe');")
        assert flagged(make_ups(db, method)._qa.check_pks("authors")) is expected


class TestCheckConstraintCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_violation_on_new_row(self, db, method, expected):
        db.execute("update staging.authors set first_name = 'Al1ce' where author_id = 'AAdams';")
        assert flagged(make_ups(db, method)._qa.check_cks("authors")) is expected

    @pytest.mark.parametrize("method", METHODS)
    def test_excluded_column_uses_base_value(self, db, method):
        """The UPDATE keeps the base value of an excluded column, so a bad staging value is irrelevant."""
        db.execute("update staging.authors set first_name = 'J0hn' where author_id = 'JDoe';")
        ups = make_ups(db, method, exclude_cols=("first_name",))
        assert flagged(ups._qa.check_cks("authors")) is False


class TestColumnExistenceSeverity:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_missing_required_column(self, db, method, expected):
        db.execute("alter table staging.authors drop column first_name;")
        errors = make_ups(db, method)._qa.check_column_existence("authors")
        assert errors, "missing column should always be reported"
        assert flagged(errors) is expected

    @pytest.mark.parametrize("method", METHODS)
    def test_missing_pk_column_is_always_error(self, db, method):
        db.execute("alter table staging.authors drop column author_id;")
        assert flagged(make_ups(db, method)._qa.check_column_existence("authors")) is True


# ===================================================================
# Unique (constraints and bare indexes)
# ===================================================================


class TestUniqueCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_new_row_collides_with_base(self, db, method, expected):
        """UNIQUE constraint: new staging author AAdams uses the email of base-only author ZOld."""
        db.execute("update public.authors set email = 'alice.adams@email.com' where author_id = 'ZOld';")
        assert flagged(make_ups(db, method)._qa.check_unique("authors")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", False), ("update", False), ("insert", True)])
    def test_freed_key_is_only_freed_when_updated(self, db, method, expected):
        """New row AAdams takes JDoe's old email; only insert mode leaves JDoe holding it."""
        db.execute("update staging.authors set email = 'jdoe.old@email.com' where author_id = 'AAdams';")
        assert flagged(make_ups(db, method)._qa.check_unique("authors")) is expected

    @pytest.mark.parametrize("method", METHODS)
    def test_updating_row_that_keeps_its_key(self, db, method):
        db.execute("update public.authors set email = 'john.doe@email.com' where author_id = 'JDoe';")
        assert make_ups(db, method)._qa.check_unique("authors") == []

    def test_describes_base_conflict(self, db):
        db.execute("update public.authors set email = 'alice.adams@email.com' where author_id = 'ZOld';")
        ups = make_ups(db, "upsert")
        ups._qa.capture_detail_rows = True
        errors = ups._qa.check_unique("authors")
        assert "1 with existing base rows" in errors[0].details
        [violation] = errors[0].violations
        assert violation.pk_values == ("AAdams",)
        assert violation.description == "duplicate unique (email); conflicts with existing base row (ZOld)"
        assert "_ups_base_keys" not in violation.row_data

    def test_bare_unique_index_duplicate_within_staging(self, db):
        db.execute("update staging.publishers set publisher_name = 'Bestseller Books' where publisher_id = 'P001';")
        errors = make_ups(db)._qa.check_unique("publishers")
        assert [e.details for e in errors] == ["uq_publishers_name (1 duplicates, 2 rows)"]

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_bare_unique_index_collides_with_base(self, db, method, expected):
        """New staging publisher P002 takes the name of base-only publisher P900."""
        db.execute("update staging.publishers set publisher_name = 'Legacy Press' where publisher_id = 'P002';")
        assert flagged(make_ups(db, method)._qa.check_unique("publishers")) is expected

    def test_partial_unique_index_is_skipped(self, db):
        """The index only covers P900 books, so duplicate notes elsewhere are legal; QA must not flag them."""
        db.execute("create unique index uq_books_notes on public.books (notes) where publisher_id = 'P900';")
        db.execute("update staging.books set notes = 'Same notes' where book_id in ('B002', 'B003');")
        assert make_ups(db)._qa.check_unique("books") == []

    def test_key_swap_is_not_flagged(self, db):
        """Known limit: the end state is valid, although a single UPDATE may still fail at load."""
        db.execute("update public.authors set email = 'alice.adams@email.com' where author_id = 'JDoe';")
        db.execute(
            "insert into public.authors (author_id, first_name, last_name, email)"
            " values ('AAdams', 'Alice', 'Adams', 'john.doe@email.com');",
        )
        assert make_ups(db, "upsert")._qa.check_unique("authors") == []


# ===================================================================
# Foreign key
# ===================================================================


class TestForeignKeyCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", False), ("update", True), ("insert", False)])
    def test_child_points_at_new_staging_parent(self, db, method, expected):
        """B001 moves to genre 'Mystery', which exists only in staging; update mode never inserts it."""
        db.execute("update staging.books set genre = 'Mystery' where book_id = 'B001';")
        assert flagged(make_ups(db, method)._qa.check_fks("books")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_unselected_staging_parent_is_not_trusted(self, db, method, expected):
        """staging.genres exists but is not being loaded, so its rows never reach the base table."""
        ups = make_ups(db, method, tables=("books",))
        assert flagged(ups._qa.check_fks("books")) is expected

    @pytest.mark.parametrize("method", METHODS)
    def test_selected_staging_parent_is_trusted(self, db, method):
        assert make_ups(db, method)._qa.check_fks("books") == []

    def test_captures_orphan_rows(self, db):
        db.execute("update staging.books set genre = 'Mystery' where book_id = 'B001';")
        ups = make_ups(db, "update")
        ups._qa.capture_detail_rows = True
        [error] = ups._qa.check_fks("books")
        assert [v.pk_values for v in error.violations] == [("B001",)]
        assert error.violations[0].row_data["genre"] == "Mystery"
