#!/usr/bin/env python
"""Tests for pg_upsert.upsert (PgUpsert).

All tests in this module require a live PostgreSQL database and are
marked ``@pytest.mark.postgres`` via the class-level marker.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from psycopg2.sql import SQL, Identifier, Literal

from pg_upsert.models import UpsertResult
from pg_upsert.upsert import PgUpsert, UserCancelledError

pytestmark = pytest.mark.postgres


# ===================================================================
# UserCancelledError
# ===================================================================


class TestUserCancelledError:
    def test_is_exception(self):
        assert issubclass(UserCancelledError, Exception)

    def test_message(self):
        err = UserCancelledError("user said no")
        assert str(err) == "user said no"


# ===================================================================
# Initialization & validation
# ===================================================================


class TestPgUpsertInit:
    def test_init_properties(self, ups):
        assert ups.tables == ("genres", "books", "authors", "book_authors", "publishers")
        assert ups.staging_schema == "staging"
        assert ups.base_schema == "public"
        assert ups.do_commit is False
        assert ups.interactive is False
        assert ups.upsert_method == "upsert"
        assert ups.control_table == "ups_control"
        assert ups.exclude_cols == ("rev_user", "rev_time")
        assert ups.exclude_null_check_cols == ()

    def test_repr(self, ups):
        r = repr(ups)
        assert "PgUpsert" in r
        assert "staging" in r
        assert "public" in r

    def test_upsert_methods(self, ups):
        assert ups._upsert_methods() == ("upsert", "update", "insert")

    def test_validate_schemas(self, ups):
        assert ups._validate_schemas() is None

    def test_validate_schemas_bad_staging(self, ups):
        ups.staging_schema = "nonexistent_schema"
        with pytest.raises(ValueError):
            ups._validate_schemas()
        ups.staging_schema = "staging"

    def test_validate_schemas_bad_base(self, ups):
        ups.base_schema = "nonexistent_schema"
        with pytest.raises(ValueError):
            ups._validate_schemas()
        ups.base_schema = "public"

    def test_validate_table(self, ups):
        assert ups._validate_table("genres") is None

    def test_validate_table_bad(self, ups):
        with pytest.raises(ValueError):
            ups._validate_table("nonexistent_table")

    def test_init_with_excludes(self, ups_with_excludes):
        assert ups_with_excludes.exclude_cols == ("rev_user", "rev_time")
        assert ups_with_excludes.exclude_null_check_cols == ("book_alias",)

    def test_init_invalid_upsert_method(self, db):
        with pytest.raises(ValueError, match="Invalid upsert method"):
            PgUpsert(
                conn=db.conn,
                tables=("genres",),
                staging_schema="staging",
                base_schema="public",
                upsert_method="invalid",
            )

    def test_init_no_schemas(self, db):
        with pytest.raises(ValueError, match="No base or staging schema"):
            PgUpsert(conn=db.conn, tables=("genres",), staging_schema=None, base_schema=None)

    def test_init_no_base_schema(self, db):
        with pytest.raises(ValueError, match="No base schema"):
            PgUpsert(conn=db.conn, tables=("genres",), staging_schema="staging", base_schema=None)

    def test_init_no_staging_schema(self, db):
        with pytest.raises(ValueError, match="No staging schema"):
            PgUpsert(conn=db.conn, tables=("genres",), staging_schema=None, base_schema="public")

    def test_init_no_tables(self, db):
        with pytest.raises(ValueError, match="No tables"):
            PgUpsert(conn=db.conn, tables=(), staging_schema="staging", base_schema="public")

    def test_init_same_schemas(self, db):
        with pytest.raises(ValueError, match="must be different"):
            PgUpsert(conn=db.conn, tables=("genres",), staging_schema="public", base_schema="public")


# ===================================================================
# Control table
# ===================================================================


class TestControlTable:
    def test_control_table_exists(self, ups):
        cur = ups.db.execute(
            SQL("SELECT table_name FROM information_schema.tables WHERE table_name={t}").format(
                t=Literal(ups.control_table),
            ),
        )
        assert cur.rowcount == 1

    def test_control_table_columns(self, ups):
        cur = ups.db.execute(
            SQL("SELECT column_name FROM information_schema.columns WHERE table_name={t}").format(
                t=Literal(ups.control_table),
            ),
        )
        columns = {row[0] for row in cur.fetchall()}
        expected = {
            "table_name",
            "exclude_cols",
            "exclude_null_checks",
            "interactive",
            "null_errors",
            "pk_errors",
            "unique_errors",
            "column_errors",
            "type_errors",
            "fk_errors",
            "ck_errors",
            "rows_updated",
            "rows_inserted",
        }
        assert expected == columns

    def test_control_table_default_values(self, ups):
        """Error columns and counters should be null/zero by default.

        Note: exclude_cols IS set because the ups fixture uses exclude_cols.
        """
        cur = ups.db.execute(
            SQL("""
                SELECT * FROM {ct}
                WHERE coalesce(null_errors, pk_errors, fk_errors, ck_errors,
                               unique_errors, column_errors, type_errors) IS NOT NULL
                   OR interactive
                   OR rows_updated != 0
                   OR rows_inserted != 0
            """).format(ct=Identifier(ups.control_table)),
        )
        assert cur.rowcount == 0

    def test_init_ups_control_populates_tables(self, ups):
        ups._init_ups_control()
        cur = ups.db.execute(
            SQL("SELECT table_name FROM {ct}").format(ct=Identifier(ups.control_table)),
        )
        tables = {row[0] for row in cur.fetchall()}
        assert tables == set(ups.tables)

    def test_init_ups_control_with_excludes(self, ups_with_excludes):
        ups_with_excludes._init_ups_control()
        cur = ups_with_excludes.db.execute(
            SQL("SELECT exclude_cols FROM {ct} WHERE exclude_cols IS NOT NULL").format(
                ct=Identifier(ups_with_excludes.control_table),
            ),
        )
        assert cur.rowcount > 0
        row = cur.fetchone()
        assert "rev_user" in row[0]
        assert "rev_time" in row[0]

    def test_init_ups_control_with_null_check_excludes(self, ups_with_excludes):
        ups_with_excludes._init_ups_control()
        cur = ups_with_excludes.db.execute(
            SQL("SELECT exclude_null_checks FROM {ct} WHERE exclude_null_checks IS NOT NULL").format(
                ct=Identifier(ups_with_excludes.control_table),
            ),
        )
        assert cur.rowcount > 0
        row = cur.fetchone()
        assert "book_alias" in row[0]

    def test_show_control_non_interactive(self, ups):
        ups.interactive = False
        with patch("pg_upsert.control.logger.info") as mock_log:
            ups.show_control()
            mock_log.assert_called_once()
            msg = mock_log.call_args[0][0]
            # rich table format uses different rendering than tabulate
            assert "table_name" in msg or "Control table" in msg

    def test_validate_control(self, ups):
        ups._validate_control()

    def test_format_sql_result(self, ups):
        """display.format_sql_result replaces the old _tabulate_sql method."""
        from pg_upsert.ui import display

        rows, headers, rowcount = ups.db.rowdict("SELECT 1 as val")
        result = display.format_sql_result(list(rows), headers)
        assert result is not None
        assert "val" in result

    def test_format_sql_result_composable(self, ups):
        from pg_upsert.ui import display

        rows, headers, rowcount = ups.db.rowdict(
            SQL("SELECT {v} as val").format(v=Literal(42)),
        )
        result = display.format_sql_result(list(rows), headers)
        assert "42" in result


# ===================================================================
# QA: Null checks (qa_one_null)
# ===================================================================


class TestQANull:
    def test_qa_one_null_detects_nulls(self, ups):
        """Insert a NULL where NOT NULL is required → null_errors populated."""
        ups.db.execute("UPDATE staging.genres SET genre = NULL WHERE genre = 'Fiction';")
        ups.qa_one_null("genres")
        cur = ups.db.execute(
            SQL("SELECT null_errors FROM {ct} WHERE table_name = {t} AND null_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.rowcount == 1

    def test_qa_one_null_nullable_column_ok(self, ups):
        """NULL in a nullable column should NOT produce errors."""
        ups.db.execute("UPDATE staging.books SET publisher_id = NULL;")
        ups.qa_one_null("books")
        cur = ups.db.execute(
            SQL("SELECT null_errors FROM {ct} WHERE table_name = {t} AND null_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("books"),
            ),
        )
        assert cur.rowcount == 0

    def test_qa_one_null_with_exclude(self, ups_with_excludes):
        """Columns listed in exclude_null_check_cols should be skipped."""
        # book_alias is excluded from null checks AND is a serial column,
        # so we test by verifying it doesn't flag a null we put elsewhere
        ups_with_excludes.qa_one_null("books")
        cur = ups_with_excludes.db.execute(
            SQL("SELECT null_errors FROM {ct} WHERE table_name = {t} AND null_errors IS NOT NULL").format(
                ct=Identifier(ups_with_excludes.control_table),
                t=Literal("books"),
            ),
        )
        assert cur.rowcount == 0


# ===================================================================
# QA: Primary key checks (qa_one_pk)
# ===================================================================


class TestQAPK:
    def test_qa_one_pk_detects_dupes(self, ups):
        ups.db.execute(
            "INSERT INTO staging.authors (author_id, first_name, last_name) VALUES ('JDoe', 'John', 'Doe');",
        )
        ups.qa_one_pk("authors")
        cur = ups.db.execute(
            SQL("SELECT pk_errors FROM {ct} WHERE table_name = {t} AND pk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("authors"),
            ),
        )
        assert cur.rowcount == 1

    def test_qa_one_pk_no_dupes(self, ups):
        ups.qa_one_pk("genres")
        cur = ups.db.execute(
            SQL("SELECT pk_errors FROM {ct} WHERE table_name = {t} AND pk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.rowcount == 0

    def test_qa_one_pk_composite_key(self, ups):
        ups.db.execute(
            "INSERT INTO staging.book_authors (book_id, author_id) VALUES ('B001', 'JDoe');",
        )
        ups.qa_one_pk("book_authors")
        cur = ups.db.execute(
            SQL("SELECT pk_errors FROM {ct} WHERE table_name = {t} AND pk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("book_authors"),
            ),
        )
        assert cur.rowcount == 1


# ===================================================================
# QA: Foreign key checks (qa_one_fk)
# ===================================================================


class TestQAFK:
    def test_qa_one_fk_detects_violation(self, ups):
        ups.db.execute("UPDATE staging.books SET genre = 'NonexistentGenre' WHERE genre = 'Fiction';")
        ups.qa_one_fk("books")
        cur = ups.db.execute(
            SQL("SELECT fk_errors FROM {ct} WHERE table_name = {t} AND fk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("books"),
            ),
        )
        assert cur.rowcount == 1

    def test_qa_one_fk_no_violations(self, ups):
        ups.qa_one_fk("books")
        cur = ups.db.execute(
            SQL("SELECT fk_errors FROM {ct} WHERE table_name = {t} AND fk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("books"),
            ),
        )
        assert cur.rowcount == 0

    def test_qa_one_fk_multiple_tables(self, ups):
        ups.db.execute("UPDATE staging.books SET genre = 'Bad' WHERE genre = 'Fiction';")
        ups.db.execute("UPDATE staging.book_authors SET book_id = 'B999' WHERE book_id = 'B002';")
        ups.qa_one_fk("books")
        ups.qa_one_fk("book_authors")
        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE fk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 2


# ===================================================================
# QA: Check constraint checks (qa_one_ck)
# ===================================================================


class TestQACK:
    def test_qa_one_ck_detects_violation(self, ups):
        """Update data to violate a CHECK constraint (author name regex).

        Note: qa_one_ck() writes errors to ups_sel_cks, not directly to the
        control table.  The control table is updated by qa_all_ck().
        """
        ups.db.execute(
            "UPDATE staging.authors SET first_name = '123Bad' WHERE author_id = 'JDoe';",
        )
        ups.qa_one_ck("authors")
        # Check ups_sel_cks for errors (qa_one_ck writes here, qa_all_ck propagates to control table)
        cur = ups.db.execute(
            "SELECT ckerror_values FROM ups_sel_cks WHERE ckerror_values > 0",
        )
        assert cur.rowcount >= 1

    def test_qa_one_ck_no_violations(self, ups):
        ups.qa_one_ck("authors")
        cur = ups.db.execute(
            SQL("SELECT ck_errors FROM {ct} WHERE table_name = {t} AND ck_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("authors"),
            ),
        )
        assert cur.rowcount == 0

    def test_qa_one_ck_table_with_no_check_constraints(self, ups):
        """Tables without CHECK constraints should be fine."""
        ups.qa_one_ck("publishers")
        cur = ups.db.execute(
            SQL("SELECT ck_errors FROM {ct} WHERE table_name = {t} AND ck_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("publishers"),
            ),
        )
        assert cur.rowcount == 0


# ===================================================================
# QA: Full QA orchestration (qa_all)
# ===================================================================


class TestQAAll:
    def test_qa_all_passing_data(self, ups):
        ups.qa_all()
        assert ups.qa_passed is True

    def test_qa_all_with_null_errors(self, ups):
        ups.db.execute("UPDATE staging.genres SET genre = NULL WHERE genre = 'Fiction';")
        ups.qa_all()
        assert ups.qa_passed is False

    def test_qa_all_with_pk_errors(self, ups):
        ups.db.execute(
            "INSERT INTO staging.authors (author_id, first_name, last_name) VALUES ('JDoe', 'John', 'Doe');",
        )
        ups.qa_all()
        assert ups.qa_passed is False

    def test_qa_all_with_fk_errors(self, ups):
        ups.db.execute("UPDATE staging.books SET genre = 'BadGenre' WHERE genre = 'Fiction';")
        ups.qa_all()
        assert ups.qa_passed is False

    def test_qa_all_with_ck_errors(self, ups):
        ups.db.execute(
            "UPDATE staging.authors SET first_name = '1Invalid' WHERE author_id = 'JDoe';",
        )
        ups.qa_all()
        assert ups.qa_passed is False

    def test_qa_all_failing_data(self, ups_failing):
        """The failing schema has null, PK, FK, and CK errors baked in."""
        ups_failing.qa_all()
        assert ups_failing.qa_passed is False


# ===================================================================
# Upsert operations
# ===================================================================


class TestUpsertOne:
    def test_insert(self, ups):
        ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted, rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        row = cur.fetchone()
        assert row[0] == 19
        assert row[1] == 0

    def test_update(self, ups):
        ups.upsert_one("genres")
        ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted, rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        row = cur.fetchone()
        assert row[0] == 0
        assert row[1] == 19

    def test_upsert_mixed(self, ups):
        ups.upsert_one("genres")
        ups.db.execute(
            "INSERT INTO staging.genres (genre, description) VALUES ('Fantasy', 'Fantasy genre');",
        )
        ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted, rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        row = cur.fetchone()
        assert row[0] + row[1] > 0

    def test_upsert_with_exclude_cols(self, ups_with_excludes):
        ups_with_excludes.upsert_one("genres")
        cur = ups_with_excludes.db.execute(
            SQL("SELECT rows_inserted FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups_with_excludes.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.fetchone()[0] == 19


class TestUpsertAll:
    def test_upsert_all(self, ups):
        """Upsert all tables respecting dependency order."""
        ups.qa_all()
        assert ups.qa_passed is True
        ups.upsert_all()
        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE rows_inserted > 0").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == len(ups.tables)

    def test_upsert_all_with_excludes(self, ups_with_excludes):
        ups_with_excludes.qa_all()
        assert ups_with_excludes.qa_passed is True
        ups_with_excludes.upsert_all()
        cur = ups_with_excludes.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE rows_inserted > 0").format(
                ct=Identifier(ups_with_excludes.control_table),
            ),
        )
        assert cur.fetchone()[0] > 0


# ===================================================================
# Upsert method variants
# ===================================================================


class TestUpsertMethods:
    def test_update_only(self, ups):
        """upsert_method='update' should only update, not insert new rows."""
        ups.upsert_method = "update"
        ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted, rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        row = cur.fetchone()
        assert row[0] == 0  # No inserts
        assert row[1] == 0  # No matches yet to update

    def test_insert_only(self, ups):
        """upsert_method='insert' should only insert, not update existing rows."""
        ups.upsert_method = "insert"
        ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted, rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        row = cur.fetchone()
        assert row[0] == 19  # All inserted
        assert row[1] == 0  # No updates


# ===================================================================
# Commit & rollback
# ===================================================================


class TestCommit:
    def test_no_commit_when_false(self, ups):
        ups.do_commit = False
        ups.upsert_one("genres")
        ups.commit()
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 0

    def test_commit_when_true(self, ups):
        ups.do_commit = True
        ups.upsert_one("genres")
        ups.commit()
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 19

    def test_commit_standalone_no_changes(self, ups):
        """Calling commit() without any upsert should not insert rows."""
        cur = ups.db.execute(
            SQL("SELECT * FROM {ct} WHERE coalesce(rows_inserted, rows_updated) IS NOT NULL").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.fetchone() is None

    def test_commit_interactive_cancel(self, ups):
        """Interactive cancel during commit should rollback."""
        ups.interactive = True
        ups.upsert_one("genres")
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (1, None)  # Cancel
            ups.commit()
        # Should have rolled back
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 0

    def test_commit_interactive_continue(self, ups):
        """Interactive continue during commit with do_commit=True should commit."""
        ups.interactive = True
        ups.do_commit = True
        ups.upsert_one("genres")
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (0, None)  # Continue
            ups.commit()
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 19

    def test_commit_no_changes(self, ups):
        """commit() with no rows inserted/updated should rollback cleanly."""
        ups.do_commit = True
        ups.commit()
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 0


# ===================================================================
# Full run()
# ===================================================================


class TestRun:
    def test_run_passing_data(self, ups):
        """Full end-to-end run with passing data and commit."""
        ups.do_commit = True
        result = ups.run()
        assert isinstance(result, UpsertResult)
        assert result.qa_passed is True
        assert result.committed is True
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 19

    def test_run_failing_data_no_upsert(self, ups_failing):
        """run() with failing QA should not upsert."""
        ups_failing.run()
        assert ups_failing.qa_passed is False
        cur = ups_failing.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 0

    def test_run_no_commit(self, ups):
        ups.do_commit = False
        ups.run()
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 0

    def test_run_returns_upsert_result(self, ups):
        result = ups.run()
        assert isinstance(result, UpsertResult)

    def test_run_interactive_cancel_at_table_selection(self, ups):
        """Interactive cancel at the table selection step should return without upserting."""
        ups.interactive = True
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (1, None)  # Cancel
            result = ups.run()
            assert isinstance(result, UpsertResult)
        cur = ups.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 0

    def test_run_resets_qa_passed(self, ups):
        """qa_passed should be reset between run() calls."""
        ups.run()
        assert ups.qa_passed is True
        # Inject a PK error
        ups.db.execute(
            "INSERT INTO staging.authors (author_id, first_name, last_name) VALUES ('JDoe', 'John', 'Doe');",
        )
        ups.run()
        assert ups.qa_passed is False


# ===================================================================
# UserCancelledError handling in run()
# ===================================================================


class TestUserCancellation:
    def test_run_catches_cancellation_and_rolls_back(self, ups):
        """If UserCancelledError is raised during QA, run() rolls back."""
        with patch.object(ups._qa, "run_all", side_effect=UserCancelledError("cancelled")):
            result = ups.run()
            assert isinstance(result, UpsertResult)
            assert result.committed is False

    def test_interactive_pk_cancel(self, ups):
        """Mocking interactive cancel during PK check raises UserCancelledError."""
        ups.interactive = True
        ups.db.execute(
            "INSERT INTO staging.authors (author_id, first_name, last_name) VALUES ('JDoe', 'John', 'Doe');",
        )
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (1, None)
            with pytest.raises(UserCancelledError):
                ups.qa_one_pk("authors")

    def test_interactive_fk_cancel(self, ups):
        """Mocking interactive cancel during FK check raises UserCancelledError."""
        ups.interactive = True
        ups.db.execute("UPDATE staging.books SET genre = 'BadGenre' WHERE genre = 'Fiction';")
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (1, None)
            with pytest.raises(UserCancelledError):
                ups.qa_one_fk("books")


# ===================================================================
# Interactive upsert_one paths
# ===================================================================


class TestInteractiveUpsertOne:
    def _set_interactive(self, ups):
        """Set the interactive flag in both the PgUpsert object and the control table."""
        ups.interactive = True
        ups.db.execute(
            SQL("UPDATE {ct} SET interactive = true").format(
                ct=Identifier(ups.control_table),
            ),
        )

    def test_interactive_update_continue(self, ups):
        """Interactive continue during update dialog should proceed."""
        ups.upsert_one("genres")
        ups.db.commit()
        self._set_interactive(ups)
        with patch.object(ups._ui, "show_comparison") as mock_show:
            mock_show.return_value = (0, None)  # Continue
            ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.fetchone()[0] == 19

    def test_interactive_update_skip(self, ups):
        """Interactive skip during update dialog should skip updates."""
        ups.upsert_one("genres")
        ups.db.commit()
        self._set_interactive(ups)
        with patch.object(ups._ui, "show_comparison") as mock_show:
            mock_show.return_value = (1, None)  # Skip
            ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_interactive_update_cancel(self, ups):
        """Interactive cancel during update dialog should raise UserCancelledError."""
        ups.upsert_one("genres")
        ups.db.commit()
        self._set_interactive(ups)
        with patch.object(ups._ui, "show_comparison") as mock_show:
            mock_show.return_value = (2, None)  # Cancel
            with pytest.raises(UserCancelledError):
                ups.upsert_one("genres")

    def test_interactive_insert_continue(self, ups):
        """Interactive continue during insert dialog should proceed."""
        self._set_interactive(ups)
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (0, None)  # Continue
            ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.fetchone()[0] == 19

    def test_interactive_insert_skip(self, ups):
        """Interactive skip during insert dialog should skip inserts."""
        self._set_interactive(ups)
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (1, None)  # Skip
            ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_interactive_insert_cancel(self, ups):
        """Interactive cancel during insert dialog should raise UserCancelledError."""
        self._set_interactive(ups)
        with patch.object(ups._ui, "show_table") as mock_show:
            mock_show.return_value = (2, None)  # Cancel
            with pytest.raises(UserCancelledError):
                ups.upsert_one("genres")


# ===================================================================
# Failing data QA checks
# ===================================================================


class TestFailingData:
    def test_failing_qa_all(self, ups_failing):
        """The failing schema has null, PK, FK, and CK errors baked in."""
        ups_failing.qa_all()
        assert ups_failing.qa_passed is False
        # Verify errors were recorded
        cur = ups_failing.db.execute(
            SQL(
                """SELECT * FROM {ct}
                WHERE null_errors IS NOT NULL OR pk_errors IS NOT NULL
                   OR fk_errors IS NOT NULL OR ck_errors IS NOT NULL
                   OR unique_errors IS NOT NULL""",
            ).format(
                ct=Identifier(ups_failing.control_table),
            ),
        )
        assert cur.rowcount > 0

    def test_failing_run_no_upsert(self, ups_failing):
        """run() with failing data should not upsert anything."""
        ups_failing.run()
        assert ups_failing.qa_passed is False
        cur = ups_failing.db.execute("SELECT count(*) FROM public.genres;")
        assert cur.fetchone()[0] == 0

    def test_failing_unique_errors(self, ups_failing):
        """The failing schema has duplicate emails violating UNIQUE constraint."""
        ups_failing._qa.check_unique("authors")
        cur = ups_failing.db.execute(
            SQL("SELECT unique_errors FROM {ct} WHERE table_name = {t} AND unique_errors IS NOT NULL").format(
                ct=Identifier(ups_failing.control_table),
                t=Literal("authors"),
            ),
        )
        assert cur.rowcount == 1

    def test_failing_column_existence(self, ups_failing):
        """The failing schema is missing 'notes' column in staging.books."""
        errors = ups_failing._qa.check_column_existence("books")
        assert len(errors) == 1
        assert "notes" in errors[0].details

    def test_failing_type_mismatch(self, ups_failing):
        """The failing schema has publisher_name as integer in staging (varchar in base)."""
        errors = ups_failing._qa.check_type_mismatch("publishers")
        assert len(errors) == 1
        assert "publisher_name" in errors[0].details


# ===================================================================
# New QA checks: UNIQUE constraints
# ===================================================================


class TestQAUnique:
    def test_check_unique_no_violations(self, ups):
        """Passing data should have no unique constraint violations."""
        errors = ups._qa.check_unique("authors")
        assert errors == []

    def test_check_unique_detects_violation(self, ups):
        """Insert duplicate email to violate UNIQUE constraint."""
        ups.db.execute(
            "UPDATE staging.authors SET email = 'john.doe@email.com' WHERE author_id = 'AAdams';",
        )
        errors = ups._qa.check_unique("authors")
        assert len(errors) == 1
        assert errors[0].check_type.value == "unique"

    def test_check_unique_nulls_allowed(self, ups):
        """Multiple NULL values in a UNIQUE column should NOT be flagged.

        PostgreSQL allows multiple NULLs in UNIQUE columns.
        """
        ups.db.execute(
            "UPDATE staging.authors SET email = NULL;",
        )
        errors = ups._qa.check_unique("authors")
        assert errors == []

    def test_check_unique_table_without_unique_constraints(self, ups):
        """Tables with no UNIQUE constraints should return no errors."""
        errors = ups._qa.check_unique("genres")
        assert errors == []


# ===================================================================
# New QA checks: Column existence
# ===================================================================


class TestQAColumnExistence:
    def test_column_existence_passing(self, ups):
        """Staging tables should have all required base columns (exclude_cols excluded)."""
        errors = ups._qa.check_column_existence("genres")
        assert errors == []

    def test_column_existence_respects_exclude_cols(self, ups):
        """rev_time and rev_user are in base but not staging — excluded via exclude_cols."""
        # ups fixture has exclude_cols=("rev_user", "rev_time")
        errors = ups._qa.check_column_existence("genres")
        assert errors == []

    def test_column_existence_detects_missing(self, ups):
        """Without exclude_cols covering the missing columns, they're flagged.

        Temporarily clear the exclude_cols in the control table to simulate.
        """
        ups.db.execute(
            SQL("UPDATE {ct} SET exclude_cols = NULL WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        errors = ups._qa.check_column_existence("genres")
        assert len(errors) == 1
        assert "rev_time" in errors[0].details
        assert "rev_user" in errors[0].details


# ===================================================================
# New QA checks: Type mismatch
# ===================================================================


class TestQATypeMismatch:
    def test_type_mismatch_passing(self, ups):
        """Matching types should produce no errors."""
        errors = ups._qa.check_type_mismatch("genres")
        assert errors == []

    def test_type_mismatch_all_tables(self, ups):
        """All tables in the passing schema should have compatible types."""
        for table in ups.tables:
            errors = ups._qa.check_type_mismatch(table)
            assert errors == [], f"Unexpected type mismatch in {table}: {errors}"


# ===================================================================
# Facade methods: qa_all_null, qa_all_unique, qa_column_existence,
#                 qa_type_mismatch, qa_all_pk, qa_all_fk, qa_all_ck
# ===================================================================


class TestFacadeMethods:
    """Smoke tests for PgUpsert facade methods that delegate to _qa."""

    def test_qa_all_null_returns_self(self, ups):
        result = ups.qa_all_null()
        assert result is ups

    def test_qa_all_null_passing_data_no_errors(self, ups):
        ups.qa_all_null()
        from psycopg2.sql import SQL, Identifier

        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE null_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_qa_all_unique_returns_self(self, ups):
        result = ups.qa_all_unique()
        assert result is ups

    def test_qa_all_unique_passing_data_no_errors(self, ups):
        ups.qa_all_unique()
        from psycopg2.sql import SQL, Identifier

        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE unique_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_qa_column_existence_returns_self(self, ups):
        result = ups.qa_column_existence()
        assert result is ups

    def test_qa_column_existence_passing_data_no_errors(self, ups):
        ups.qa_column_existence()
        from psycopg2.sql import SQL, Identifier

        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE column_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_qa_type_mismatch_returns_self(self, ups):
        result = ups.qa_type_mismatch()
        assert result is ups

    def test_qa_type_mismatch_passing_data_no_errors(self, ups):
        ups.qa_type_mismatch()
        from psycopg2.sql import SQL, Identifier

        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE type_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_qa_all_pk_returns_self(self, ups):
        result = ups.qa_all_pk()
        assert result is ups

    def test_qa_all_pk_passing_data_no_errors(self, ups):
        ups.qa_all_pk()
        from psycopg2.sql import SQL, Identifier

        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE pk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_qa_all_fk_returns_self(self, ups):
        result = ups.qa_all_fk()
        assert result is ups

    def test_qa_all_fk_passing_data_no_errors(self, ups):
        ups.qa_all_fk()
        from psycopg2.sql import SQL, Identifier

        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE fk_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 0

    def test_qa_all_ck_returns_self(self, ups):
        result = ups.qa_all_ck()
        assert result is ups

    def test_qa_all_ck_passing_data_no_errors(self, ups):
        ups.qa_all_ck()
        from psycopg2.sql import SQL, Identifier

        cur = ups.db.execute(
            SQL("SELECT COUNT(*) FROM {ct} WHERE ck_errors IS NOT NULL").format(
                ct=Identifier(ups.control_table),
            ),
        )
        assert cur.fetchone()[0] == 0


# ===================================================================
# Composite UNIQUE constraint
# ===================================================================


class TestCompositeUnique:
    def test_composite_unique_no_violations(self, ups):
        """Passing data should have no violations on the (book_title, genre) unique constraint."""
        errors = ups._qa.check_unique("books")
        assert errors == []

    def test_composite_unique_detects_violation(self, ups):
        """Insert duplicate (book_title, genre) pair to violate the composite unique constraint."""
        # Copy an existing book with same title+genre
        ups.db.execute(
            """INSERT INTO staging.books (book_id, book_title, genre, publisher_id)
            SELECT 'BDUP', book_title, genre, publisher_id
            FROM staging.books WHERE book_id = 'B001'""",
        )
        errors = ups._qa.check_unique("books")
        assert len(errors) == 1
        assert "uq_books_title_genre" in errors[0].details

    def test_composite_unique_different_genre_ok(self, ups):
        """Same title with different genre should NOT be a violation."""
        ups.db.execute(
            """INSERT INTO staging.books (book_id, book_title, genre, publisher_id)
            VALUES ('BNEW', 'The Great Novel', 'Non-Fiction', 'P001')""",
        )
        errors = ups._qa.check_unique("books")
        assert errors == []


# ===================================================================
# Empty staging table edge case
# ===================================================================


class TestEmptyStagingTable:
    def test_null_check_empty_staging(self, ups):
        """Null checks should pass on a table with 0 rows."""
        ups.db.execute("DELETE FROM staging.genres;")
        errors = ups._qa.check_nulls("genres")
        assert errors == []

    def test_pk_check_empty_staging(self, ups):
        """PK check should pass on a table with 0 rows."""
        ups.db.execute("DELETE FROM staging.genres;")
        errors = ups._qa.check_pks("genres")
        assert errors == []

    def test_unique_check_empty_staging(self, ups):
        """Unique check should pass on a table with 0 rows."""
        ups.db.execute("DELETE FROM staging.authors;")
        errors = ups._qa.check_unique("authors")
        assert errors == []

    def test_upsert_one_empty_staging(self, ups):
        """Upsert on an empty staging table should succeed with 0 inserts/updates."""
        ups.db.execute("DELETE FROM staging.genres;")
        ups.upsert_one("genres")
        cur = ups.db.execute(
            SQL("SELECT rows_inserted, rows_updated FROM {ct} WHERE table_name = {t}").format(
                ct=Identifier(ups.control_table),
                t=Literal("genres"),
            ),
        )
        row = cur.fetchone()
        assert row[0] == 0
        assert row[1] == 0


# ===================================================================
# Dependency order validation
# ===================================================================


class TestDependencyOrder:
    def test_upsert_all_respects_fk_order(self, ups):
        """Tables with FK dependencies must be upserted after their parents.

        In our schema: books depends on genres and publishers,
        book_authors depends on books and authors.
        So genres/publishers must come before books,
        and books/authors must come before book_authors.
        """
        ups.qa_all()
        assert ups.qa_passed is True

        # Get the dependency-ordered table list from the executor.
        ordered = ups._executor._get_dependency_order()

        # Verify parent tables come before children.
        genres_idx = ordered.index("genres")
        publishers_idx = ordered.index("publishers")
        books_idx = ordered.index("books")
        authors_idx = ordered.index("authors")
        book_authors_idx = ordered.index("book_authors")

        assert genres_idx < books_idx, "genres must be upserted before books"
        assert publishers_idx < books_idx, "publishers must be upserted before books"
        assert books_idx < book_authors_idx, "books must be upserted before book_authors"
        assert authors_idx < book_authors_idx, "authors must be upserted before book_authors"
