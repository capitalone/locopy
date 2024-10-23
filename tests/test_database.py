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

import sqlite3
from unittest import mock

import pandas as pd
import pg8000
import polars as pl
import polars.testing as pltest
import psycopg2
import pytest
import snowflake.connector
from locopy import Database
from locopy.errors import CredentialsError, DBError

GOOD_CONFIG_YAML = """
host: host
port: 1234
database: database
user: id
password: pass
other: stuff
extra: 123
another: 321"""

DBAPIS = [sqlite3, pg8000, psycopg2, snowflake.connector]


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_database_constructor(credentials, dbapi):
    d = Database(dbapi=dbapi, **credentials)
    assert d.connection["host"] == "host"
    assert d.connection["port"] == "port"
    assert d.connection["database"] == "database"
    assert d.connection["user"] == "user"
    assert d.connection["password"] == "password"


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_database_constructor_kwargs(dbapi):
    d = Database(
        dbapi=dbapi,
        host="host",
        port="port",
        database="database",
        user="user",
        password="password",
    )
    assert d.connection["host"] == "host"
    assert d.connection["port"] == "port"
    assert d.connection["database"] == "database"
    assert d.connection["user"] == "user"
    assert d.connection["password"] == "password"


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
def test_database_constructor_kwargs_and_yaml(dbapi):
    with pytest.raises(CredentialsError):
        Database(
            dbapi=dbapi,
            config_yaml="some_config.yml",
            host="host",
            port="port",
            database="database",
            user="user",
            password="password",
        )


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_database_constructor_with_extras(credentials, dbapi):
    credentials["extra"] = 123
    credentials["another"] = 321
    d = Database(dbapi=dbapi, **credentials)
    assert d.connection["host"] == "host"
    assert d.connection["port"] == "port"
    assert d.connection["database"] == "database"
    assert d.connection["user"] == "user"
    assert d.connection["password"] == "password"
    assert d.connection["extra"] == 123
    assert d.connection["another"] == 321


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
def test_database_constructor_yaml(dbapi):
    d = Database(dbapi=dbapi, config_yaml="some_config.yml")
    assert d.connection["host"] == "host"
    assert d.connection["port"] == 1234
    assert d.connection["database"] == "database"
    assert d.connection["user"] == "id"
    assert d.connection["password"] == "pass"
    assert d.connection["other"] == "stuff"
    assert d.connection["extra"] == 123
    assert d.connection["another"] == 321


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_is_connected(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        b = Database(dbapi=dbapi, **credentials)
        assert b._is_connected() is False

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        b = Database(dbapi=dbapi, **credentials)
        b.connect()
        assert b._is_connected() is True

    # throws exception in _is_connected
    b = Database(dbapi=dbapi, **credentials)
    del b.conn
    assert b._is_connected() is False


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_connect(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        b = Database(dbapi=dbapi, **credentials)
        b.connect()
        mock_connect.assert_called_with(
            host="host",
            user="user",
            port="port",
            password="password",
            database="database",
        )

    credentials["extra"] = 123
    credentials["another"] = 321
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        b = Database(dbapi=dbapi, **credentials)
        b.connect()
        mock_connect.assert_called_with(
            host="host",
            user="user",
            port="port",
            password="password",
            database="database",
            extra=123,
            another=321,
        )

    # side effect exception
    mock_connect.side_effect = Exception("Connect Exception")
    with pytest.raises(DBError):
        b.connect()


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_disconnect(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        b = Database(dbapi=dbapi, **credentials)
        b.connect()
        b.disconnect()
        b.conn.close.assert_called_with()
        b.cursor.close.assert_called_with()

    # side effect exception
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        b.connect()
        b.conn.close.side_effect = Exception("Disconnect Exception")
        with pytest.raises(DBError):
            b.disconnect()


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_disconnect_no_conn(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        b = Database(dbapi=dbapi, **credentials)
        b.disconnect()


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_execute(credentials, dbapi):
    with (
        mock.patch(dbapi.__name__ + ".connect") as mock_connect,
        Database(dbapi=dbapi, **credentials) as test,
    ):
        test.execute("SELECT * FROM some_table")
        assert test.cursor.execute.called is True


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_execute_no_connection_exception(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test = Database(dbapi=dbapi, **credentials)
        test.conn = None
        test.cursor = None
        with pytest.raises(DBError):
            test.execute("SELECT * FROM some_table")


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_execute_sql_exception(credentials, dbapi):
    with (
        mock.patch(dbapi.__name__ + ".connect") as mock_connect,
        Database(dbapi=dbapi, **credentials) as test,
    ):
        test.cursor.execute.side_effect = Exception("SQL Exception")
        with pytest.raises(DBError):
            test.execute("SELECT * FROM some_table")


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_to_dataframe_all_pandas(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchall.return_value = [
            (1, 2),
            (2, 3),
            (3,),
        ]
        expected_df = pd.DataFrame(
            [
                (1, 2),
                (2, 3),
                (3,),
            ]
        )
        with Database(dbapi=dbapi, **credentials) as test:
            test.execute("SELECT 'hello world' AS fld")
            df = test.to_dataframe(df_type="pandas")
        pd.testing.assert_frame_equal(df, expected_df)
    assert mock_connect.return_value.cursor.return_value.fetchall.called


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_to_dataframe_custom_size(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchmany.return_value = [
            (1, 2),
            (2, 3),
            (3,),
        ]
        expected_df = pd.DataFrame(
            [
                (1, 2),
                (2, 3),
                (3,),
            ]
        )
        with Database(dbapi=dbapi, **credentials) as test:
            test.execute("SELECT 'hello world' AS fld")
            df = test.to_dataframe(size=5)
        pd.testing.assert_frame_equal(df, expected_df)
    mock_connect.return_value.cursor.return_value.fetchmany.assert_called_with(5)


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("pandas.DataFrame")
def test_to_dataframe_none(mock_pandas, credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchmany.return_value = []
        with Database(dbapi=dbapi, **credentials) as test:
            test.execute("SELECT 'hello world' WHERE 1=0")
            assert test.to_dataframe(size=5) is None
            mock_pandas.assert_not_called()


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_to_dataframe_all_polars(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchall.return_value = [
            (1, 2),
            (2, 3),
            (3, 4),
        ]
        expected_df = pl.DataFrame([[1, 2, 3], [2, 3, 4]])
        with Database(dbapi=dbapi, **credentials) as test:
            test.execute("SELECT 'hello world' AS fld")
            df = test.to_dataframe(df_type="polars")
        pltest.assert_frame_equal(df, expected_df)

    assert mock_connect.return_value.cursor.return_value.fetchall.called


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_to_dataframe_error(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchall.return_value = [
            (1, 2),
            (2, 3),
            (3, 4),
        ]
        with Database(dbapi=dbapi, **credentials) as test:
            test.execute("SELECT 'hello world' AS fld")
            with pytest.raises(ValueError):
                test.to_dataframe(df_type="invalid")


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_get_column_names(credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.description = [["COL1"], ["COL2"]]
        with Database(dbapi=dbapi, **credentials) as test:
            assert test.column_names() == ["col1", "col2"]

        mock_connect.return_value.cursor.return_value.description = [
            ("COL1",),
            ("COL2",),
        ]
        with Database(dbapi=dbapi, **credentials) as test:
            assert test.column_names() == ["col1", "col2"]

        mock_connect.return_value.cursor.return_value.description = (
            ("COL1",),
            ("COL2",),
        )
        with Database(dbapi=dbapi, **credentials) as test:
            assert test.column_names() == ["col1", "col2"]


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_to_dict(credentials, dbapi):
    def cols():
        return ["col1", "col2"]

    test = Database(dbapi=dbapi, **credentials)

    test.column_names = cols
    test.cursor = [(1, 2), (2, 3), (3,)]
    assert list(test.to_dict()) == [
        {"col1": 1, "col2": 2},
        {"col1": 2, "col2": 3},
        {"col1": 3},
    ]

    test.column_names = cols
    test.cursor = [("a", 2), (2, 3), ("b",)]
    assert list(test.to_dict()) == [
        {"col1": "a", "col2": 2},
        {"col1": 2, "col2": 3},
        {"col1": "b"},
    ]
