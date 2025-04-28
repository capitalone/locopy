# MIT License
#
# Copyright (c) 2018 Capital One Services, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import pg8000
import psycopg2
import pytest
from unittest import mock
import pandas as pd
from locopy import PostgreSQL

DBAPIS = [pg8000, psycopg2]

@pytest.mark.parametrize("dbapi", DBAPIS)
def test_postgres_connect(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pg = PostgreSQL(dbapi=dbapi, **credentials)
        pg.connect()

        # Instead of assert_called_with, check the call_args dict for expected keys/values
        call_args = mock_connect.call_args.kwargs
        assert call_args["host"] == credentials["host"]
        assert call_args["user"] == credentials["user"]
        assert call_args["port"] == credentials["port"]
        assert call_args["password"] == credentials["password"]
        assert call_args["database"] == credentials["database"]
        if dbapi.__name__ == "psycopg2":
            assert call_args["sslmode"] == "prefer"
        if dbapi.__name__ == "pg8000":
            assert call_args["ssl_context"] is True
        mock_conn.cursor.assert_called_with()
        # side effect exception
        mock_connect.side_effect = Exception("Connect Exception")
        with pytest.raises(Exception):
            pg.connect()

@pytest.mark.parametrize("dbapi", DBAPIS)
def test_postgres_execute(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pg = PostgreSQL(dbapi=dbapi, **credentials)
        pg.connect()
        with mock.patch.object(pg, "_is_connected", return_value=True):
            pg.execute("SELECT 1")
            mock_cursor.execute.assert_called_once_with("SELECT 1", ())
            mock_conn.commit.assert_called_once()

@pytest.mark.parametrize("dbapi", DBAPIS)
def test_postgres_insert_dataframe_to_table(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pg = PostgreSQL(dbapi=dbapi, **credentials)
        pg.connect()
        with mock.patch.object(pg, "_is_connected", return_value=True):
            df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
            pg.insert_dataframe_to_table(df, "test_table")
            assert mock_cursor.execute.call_count > 0 or mock_cursor.executemany.call_count > 0
            mock_conn.commit.assert_called()

@pytest.mark.parametrize("dbapi", DBAPIS)
def test_postgres_create_table_and_insert(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        pg = PostgreSQL(dbapi=dbapi, **credentials)
        pg.connect()
        with mock.patch.object(pg, "_is_connected", return_value=True):
            df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
            metadata = {"a": "int", "b": "varchar"}
            pg.insert_dataframe_to_table(df, "test_table", create=True, metadata=metadata)
            assert mock_cursor.execute.call_count > 0 or mock_cursor.executemany.call_count > 0
            mock_conn.commit.assert_called() 