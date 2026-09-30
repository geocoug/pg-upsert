#!/usr/bin/env python
"""Tests for method-aware QA: checks only consider rows ``upsert_all()`` will write.

Each scenario seeds one or more base rows so that some staging rows would be
updated and others inserted, then runs the check under each upsert method.
"""

from __future__ import annotations

import pytest
from psycopg.sql import SQL

from pg_upsert.upsert import PgUpsert

pytestmark = pytest.mark.postgres

METHODS = ("upsert", "update", "insert")
TABLES = ("genres", "books", "authors", "book_authors", "publishers")


def make_ups(db, method: str = "upsert", exclude_cols=("rev_user", "rev_time"), tables=TABLES) -> PgUpsert:
    """Build a PgUpsert against the passing-data database."""
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


def seed_base_author(db, author_id: str = "JDoe", email: str | None = "old.address@email.com") -> None:
    """Insert one row into public.authors so its staging twin becomes an update."""
    db.execute(
        "insert into public.authors (author_id, first_name, last_name, email) values (%s, 'Base', 'Row', %s);",
        (author_id, email),
    )


def effective(ups: PgUpsert, table: str, cols: str) -> list[tuple]:
    rows = ups.db.execute(
        SQL("select {cols} from {src} order by 1").format(
            cols=SQL(cols),
            src=ups._qa._effective_rows(table),
        ),
    ).fetchall()
    return [tuple(r) for r in rows]


# ===================================================================
# Effective / predicted rows
# ===================================================================


class TestEffectiveRows:
    @pytest.mark.parametrize(
        ("method", "expected_count", "jdoe_included"),
        [("upsert", 13, True), ("update", 1, True), ("insert", 12, False)],
    )
    def test_rows_follow_method(self, db, method, expected_count, jdoe_included):
        seed_base_author(db)
        ups = make_ups(db, method)
        rows = effective(ups, "authors", "author_id")
        assert len(rows) == expected_count
        assert (("JDoe",) in rows) is jdoe_included

    def test_upsert_without_excludes_is_staging_table(self, db):
        ups = make_ups(db, "upsert")
        src = ups._qa._effective_rows("authors").as_string(db.conn)
        assert src == '"staging"."authors" as "s"'

    def test_excluded_column_uses_base_value_on_update(self, db):
        seed_base_author(db, email="old.address@email.com")
        ups = make_ups(db, "upsert", exclude_cols=("email",))
        rows = dict(effective(ups, "authors", "author_id, email"))
        assert rows["JDoe"] == "old.address@email.com"

    def test_excluded_column_is_null_on_insert(self, db):
        seed_base_author(db)
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
            ("insert", "old.address@email.com"),
        ],
    )
    def test_base_row_replaced_only_when_updated(self, db, method, expected_jdoe_email):
        seed_base_author(db, email="old.address@email.com")
        ups = make_ups(db, method)
        rows = db.execute(
            SQL("select author_id, email, _ups_src from {src} where author_id = 'JDoe'").format(
                src=ups._qa._predicted_rows("authors", ["email"]),
            ),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == expected_jdoe_email

    def test_untouched_base_rows_are_kept(self, db):
        seed_base_author(db, author_id="ZZBase", email="zz@email.com")
        ups = make_ups(db, "update")
        rows = db.execute(
            SQL("select author_id, _ups_src from {src}").format(
                src=ups._qa._predicted_rows("authors", ["email"]),
            ),
        ).fetchall()
        assert ("ZZBase", "base") in [tuple(r) for r in rows]


# ===================================================================
# Row-local checks
# ===================================================================


def flagged(errors) -> bool:
    return any(e.severity.value == "error" for e in errors)


class TestNullCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_null_on_new_row(self, db, method, expected):
        seed_base_author(db)
        db.execute("update staging.authors set first_name = null where author_id = 'AAdams';")
        assert flagged(make_ups(db, method)._qa.check_nulls("authors")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", True), ("insert", False)])
    def test_null_on_existing_row(self, db, method, expected):
        seed_base_author(db)
        db.execute("update staging.authors set first_name = null where author_id = 'JDoe';")
        assert flagged(make_ups(db, method)._qa.check_nulls("authors")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_excluded_required_column_is_null_on_insert(self, db, method, expected):
        """An excluded NOT NULL column without a default is NULL in every inserted row."""
        seed_base_author(db)
        ups = make_ups(db, method, exclude_cols=("first_name",))
        assert flagged(ups._qa.check_nulls("authors")) is expected


class TestLengthCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", True), ("insert", False)])
    def test_overflow_on_existing_row(self, db, method, expected):
        seed_base_author(db)
        db.execute("update staging.authors set email = repeat('x', 101) where author_id = 'JDoe';")
        assert flagged(make_ups(db, method)._qa.check_lengths("authors")) is expected


class TestPrimaryKeyCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_duplicate_new_key(self, db, method, expected):
        seed_base_author(db)
        db.execute("insert into staging.authors (author_id, first_name, last_name) values ('AAdams', 'Al', 'Adams');")
        assert flagged(make_ups(db, method)._qa.check_pks("authors")) is expected

    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", True), ("insert", False)])
    def test_duplicate_existing_key(self, db, method, expected):
        seed_base_author(db)
        db.execute("insert into staging.authors (author_id, first_name, last_name) values ('JDoe', 'Jon', 'Doe');")
        assert flagged(make_ups(db, method)._qa.check_pks("authors")) is expected


class TestCheckConstraintCheck:
    @pytest.mark.parametrize(("method", "expected"), [("upsert", True), ("update", False), ("insert", True)])
    def test_violation_on_new_row(self, db, method, expected):
        seed_base_author(db)
        db.execute("update staging.authors set first_name = 'Al1ce' where author_id = 'AAdams';")
        assert flagged(make_ups(db, method)._qa.check_cks("authors")) is expected

    @pytest.mark.parametrize("method", METHODS)
    def test_excluded_column_uses_base_value(self, db, method):
        """The UPDATE keeps the base value of an excluded column, so a bad staging value is irrelevant."""
        seed_base_author(db)
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
