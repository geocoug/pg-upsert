#!/usr/bin/env python
"""Tests for pg_upsert.postgres (PostgresDB).

Integration tests are marked ``@pytest.mark.postgres`` and auto-skip
when no database is reachable.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from psycopg2.sql import SQL, Identifier, Literal

from pg_upsert import PostgresDB

# ===================================================================
# Unit tests (no database required)
# ===================================================================


class TestPostgresDBInit:
    def test_no_conn_no_uri_raises(self):
        with pytest.raises(AttributeError, match="Either a connection URI"):
            PostgresDB()

    def test_bad_uri_raises(self):
        with pytest.raises(psycopg2.Error):
            PostgresDB(uri="postgresql://nouser:nopass@127.0.0.1:59999/nodb")

    def test_conn_and_uri_prefers_conn(self):
        """When both conn and uri are given, uri is ignored."""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.get_dsn_parameters.return_value = {"host": "localhost"}
        with patch("psycopg2.connect") as mock_connect:
            db = PostgresDB(uri="postgresql://fake", conn=mock_conn)
            mock_connect.assert_not_called()
            assert db.conn is mock_conn

    def test_password_prompt_when_missing(self):
        """If URI has no password, getpass is called."""
        with (
            patch("getpass.getpass", return_value="secret") as mock_gp,
            patch("psycopg2.connect") as mock_connect,
        ):
            mock_conn = MagicMock()
            mock_conn.closed = False
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn
            PostgresDB(uri="postgresql://user@localhost:5432/db")
            mock_gp.assert_called_once()

    def test_password_present_no_prompt(self):
        """If URI already contains a password, getpass is NOT called."""
        with patch("getpass.getpass") as mock_gp, patch("psycopg2.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_conn.closed = False
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_connect.return_value = mock_conn
            PostgresDB(uri="postgresql://user:pass@localhost:5432/db")
            mock_gp.assert_not_called()

    def test_keyboard_interrupt_during_password(self):
        with patch("getpass.getpass", side_effect=KeyboardInterrupt), pytest.raises(KeyboardInterrupt):
            PostgresDB(uri="postgresql://user@localhost:5432/db")

    def test_eof_error_during_password(self):
        with patch("getpass.getpass", side_effect=EOFError), pytest.raises(EOFError):
            PostgresDB(uri="postgresql://user@localhost:5432/db")


class TestPostgresDBRepr:
    def test_repr_with_conn(self):
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_conn.get_dsn_parameters.return_value = {"host": "localhost", "dbname": "test"}
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        db = PostgresDB.__new__(PostgresDB)
        db.conn = mock_conn
        db.encoding = "utf-8"
        db.in_transaction = False
        db.kwargs = {}
        assert "PostgresDB" in repr(db)
        assert "localhost" in repr(db)


class TestPostgresDBMethods:
    def _make_db(self, owns_connection=True):
        db = PostgresDB.__new__(PostgresDB)
        db.conn = MagicMock()
        db.conn.closed = False
        db.encoding = "utf-8"
        db.in_transaction = False
        db.kwargs = {}
        db._owns_connection = owns_connection
        return db

    def test_commit_when_in_transaction(self):
        db = self._make_db()
        db.in_transaction = True
        db.commit()
        db.conn.commit.assert_called_once()
        assert db.in_transaction is False

    def test_commit_when_not_in_transaction(self):
        db = self._make_db()
        db.in_transaction = False
        db.commit()
        db.conn.commit.assert_not_called()

    def test_rollback_when_in_transaction(self):
        db = self._make_db()
        db.in_transaction = True
        db.rollback()
        db.conn.rollback.assert_called_once()
        assert db.in_transaction is False

    def test_rollback_when_not_in_transaction(self):
        db = self._make_db()
        db.in_transaction = False
        db.rollback()
        db.conn.rollback.assert_not_called()

    def test_close(self):
        db = self._make_db()
        db.close()
        db.conn.close.assert_called_once()

    def test_close_already_closed(self):
        db = self._make_db()
        db.conn.closed = True
        db.close()
        db.conn.close.assert_not_called()

    def test_close_skips_external_connection(self):
        db = self._make_db(owns_connection=False)
        db.close()
        db.conn.close.assert_not_called()

    def test_del_closes_open_connection(self):
        db = self._make_db()
        db.__del__()
        db.conn.close.assert_called_once()

    def test_del_skips_external_connection(self):
        db = self._make_db(owns_connection=False)
        db.__del__()
        db.conn.close.assert_not_called()

    def test_del_no_conn(self):
        db = PostgresDB.__new__(PostgresDB)
        # Should not raise
        db.__del__()


# ===================================================================
# Integration tests (require live PostgreSQL)
# ===================================================================


@pytest.mark.postgres
class TestPostgresDBIntegration:
    def test_connect_with_uri(self, global_variables):
        db = PostgresDB(uri=global_variables["URI"])
        assert db.conn is not None
        assert not db.conn.closed
        db.close()

    def test_connect_with_existing_conn(self, global_variables):
        conn = psycopg2.connect(global_variables["URI"])
        db = PostgresDB(conn=conn)
        assert db.conn is conn
        db.close()

    def test_connect_with_password_prompt(self, global_variables):
        uri = f"postgresql://{global_variables['POSTGRES_USER']}@{global_variables['POSTGRES_HOST']}:{global_variables['POSTGRES_PORT']}/{global_variables['POSTGRES_DB']}"
        with patch("getpass.getpass", return_value=global_variables["POSTGRES_PASSWORD"]):
            db = PostgresDB(uri=uri)
            assert not db.conn.closed
            db.close()

    def test_execute_simple(self, db):
        cur = db.execute("SELECT 1")
        assert cur.fetchone()[0] == 1

    def test_execute_composable(self, db):
        cur = db.execute(SQL("SELECT {}").format(Literal(42)))
        assert cur.fetchone()[0] == 42

    def test_execute_with_identifiers(self, db):
        cur = db.execute(
            SQL(
                "SELECT {column} FROM information_schema.tables WHERE table_schema={schema} AND {column}={value}",
            ).format(
                column=Identifier("table_name"),
                schema=Literal("staging"),
                value=Literal("genres"),
            ),
        )
        assert cur.fetchone()[0] == "genres"

    def test_rowdict(self, db):
        rows, headers, rowcount = db.rowdict("SELECT 1 as one, 2 as two")
        assert headers == ["one", "two"]
        assert rowcount == 1
        rows_list = list(rows)
        assert rows_list[0]["one"] == 1
        assert rows_list[0]["two"] == 2

    def test_rowdict_empty(self, db):
        rows, headers, rowcount = db.rowdict("SELECT 1 as one WHERE 1=0")
        assert list(rows) == []
        assert rowcount == 0

    def test_rowdict_composable(self, db):
        rows, headers, rowcount = db.rowdict(
            SQL("SELECT {one} as one").format(one=Literal(99)),
        )
        rows_list = list(rows)
        assert rows_list[0]["one"] == 99

    def test_execute_rollback_on_error(self, db):
        with pytest.raises(psycopg2.errors.UndefinedTable):
            db.execute("SELECT * FROM nonexistent_table_xyz")
        assert db.in_transaction is False

    def test_commit_and_rollback(self, db):
        """Integration test for commit/rollback with real transactions."""
        db.execute("CREATE TEMPORARY TABLE test_txn (id int)")
        db.execute("INSERT INTO test_txn VALUES (1)")
        assert db.in_transaction is True
        db.commit()
        assert db.in_transaction is False
        db.execute("INSERT INTO test_txn VALUES (2)")
        db.rollback()
        cur = db.execute("SELECT count(*) FROM test_txn")
        assert cur.fetchone()[0] == 1  # Only the committed row

    def test_repr(self, db):
        r = repr(db)
        assert "PostgresDB" in r
