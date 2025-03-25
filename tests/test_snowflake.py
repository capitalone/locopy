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


import os
from collections import OrderedDict
from pathlib import PureWindowsPath
from unittest import mock

import hypothesis.strategies as s
import locopy
import polars as pl
import pyarrow as pa
import pytest
import snowflake.connector
from hypothesis import HealthCheck, given, settings
from locopy import Snowflake
from locopy.errors import DBError
from polars.testing import assert_frame_equal

PROFILE = "test"
KMS = "kms_test"

GOOD_CONFIG_YAML = """
account: account
warehouse: warehouse
database: database
user: user
password: password"""

DBAPIS = snowflake.connector

LIST_STRATEGY = s.lists(s.characters(blacklist_characters=" "), max_size=10)
CHAR_STRATEGY = s.characters()


@given(LIST_STRATEGY)
def test_random_list_combine(input_list):
    """This function tests the combine_options function using random lists"""
    output = locopy.snowflake.combine_options(input_list)
    assert isinstance(output, str)
    if input_list:
        assert len(output.split(" ")) == len(input_list)


CURR_DIR = os.path.dirname(os.path.abspath(__file__))


def test_combine_options():
    assert locopy.snowflake.combine_options(None) == ""
    assert locopy.snowflake.combine_options(["a", "b", "c"]) == "a b c"
    assert locopy.snowflake.combine_options(["a=1", "b=2", "c=3"]) == "a=1 b=2 c=3"
    assert locopy.snowflake.combine_options([""]) == ""
    with pytest.raises(TypeError):
        assert locopy.snowflake.combine_options([1])


@mock.patch("locopy.s3.Session")
@given(input_kms_key=CHAR_STRATEGY, profile=CHAR_STRATEGY)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_constructor(input_kms_key, profile, mock_session, sf_credentials):
    sf = Snowflake(
        profile=profile, kms_key=input_kms_key, dbapi=DBAPIS, **sf_credentials
    )
    mock_session.assert_called_with(profile_name=profile)
    assert sf.profile == profile
    assert sf.kms_key == input_kms_key
    assert sf.connection["account"] == "account"
    assert sf.connection["warehouse"] == "warehouse"
    assert sf.connection["database"] == "database"
    assert sf.connection["schema"] == "schema"
    assert sf.connection["user"] == "user"
    assert sf.connection["password"] == "password"


@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
@mock.patch("locopy.s3.Session")
@given(input_kms_key=CHAR_STRATEGY, profile=CHAR_STRATEGY)
def test_constructor_yaml(input_kms_key, profile, mock_session):
    sf = Snowflake(
        profile=profile,
        kms_key=input_kms_key,
        dbapi=DBAPIS,
        config_yaml="some_config.yml",
    )
    mock_session.assert_called_with(profile_name=profile)
    assert sf.profile == profile
    assert sf.kms_key == input_kms_key
    assert sf.connection["account"] == "account"
    assert sf.connection["warehouse"] == "warehouse"
    assert sf.connection["database"] == "database"
    assert sf.connection["user"] == "user"
    assert sf.connection["password"] == "password"


@mock.patch("locopy.s3.Session")
def test_connect(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf = Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials)
        sf.connect()

        mock_connect.assert_called_with(
            account="account",
            user="user",
            warehouse="warehouse",
            password="password",
            database="database",
            schema="schema",
        )
        sf.conn.cursor.assert_called_with()
        sf.conn.cursor.return_value.execute.assert_any_call(
            "USE WAREHOUSE warehouse", ()
        )
        sf.conn.cursor.return_value.execute.assert_any_call("USE DATABASE database", ())
        sf.conn.cursor.return_value.execute.assert_any_call("USE SCHEMA schema", ())

        # side effect exception
        mock_connect.side_effect = Exception("Connect Exception")
        with pytest.raises(DBError):
            sf.connect()


@mock.patch("locopy.s3.Session")
def test_with_connect(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:
            mock_connect.assert_called_with(
                account="account",
                user="user",
                warehouse="warehouse",
                password="password",
                database="database",
                schema="schema",
            )
            sf.conn.cursor.assert_called_with()
            sf.conn.cursor.return_value.execute.assert_any_call(
                "USE WAREHOUSE warehouse", ()
            )
            sf.conn.cursor.return_value.execute.assert_any_call(
                "USE DATABASE database", ()
            )
            sf.conn.cursor.return_value.execute.assert_any_call("USE SCHEMA schema", ())

        mock_connect.side_effect = Exception("Connect Exception")
        with (
            pytest.raises(DBError),
            Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
        ):
            sf.cursor  # noqa: B018


@mock.patch("locopy.s3.Session")
def test_upload_to_internal(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.upload_to_internal("/some/file", "@~/internal")
        sf.conn.cursor.return_value.execute.assert_called_with(
            "PUT 'file:///some/file' @~/internal PARALLEL=4 AUTO_COMPRESS=True OVERWRITE=True",
            (),
        )

        sf.upload_to_internal(
            "/some/file", "@~/internal", parallel=99, auto_compress=False
        )
        sf.conn.cursor.return_value.execute.assert_called_with(
            "PUT 'file:///some/file' @~/internal PARALLEL=99 AUTO_COMPRESS=False OVERWRITE=True",
            (),
        )

        sf.upload_to_internal("/some/file", "@~/internal", overwrite=False)
        sf.conn.cursor.return_value.execute.assert_called_with(
            "PUT 'file:///some/file' @~/internal PARALLEL=4 AUTO_COMPRESS=True OVERWRITE=False",
            (),
        )

        # exception
        sf.conn.cursor.return_value.execute.side_effect = Exception("PUT Exception")
        with pytest.raises(DBError):
            sf.upload_to_internal("/some/file", "@~/internal")


@mock.patch("locopy.snowflake.PurePath", new=PureWindowsPath)
@mock.patch("locopy.s3.Session")
def test_upload_to_internal_windows(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.upload_to_internal(r"C:\some\file", "@~/internal")
        sf.conn.cursor.return_value.execute.assert_called_with(
            "PUT 'file://C:/some/file' @~/internal PARALLEL=4 AUTO_COMPRESS=True OVERWRITE=True",
            (),
        )


@mock.patch("locopy.s3.Session")
def test_download_from_internal(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.download_from_internal("@~/internal", "/some/file")
        sf.conn.cursor.return_value.execute.assert_called_with(
            "GET @~/internal 'file:///some/file' PARALLEL=10", ()
        )

        sf.download_from_internal("@~/internal", "/some/file", parallel=99)
        sf.conn.cursor.return_value.execute.assert_called_with(
            "GET @~/internal 'file:///some/file' PARALLEL=99", ()
        )

        # exception
        sf.conn.cursor.return_value.execute.side_effect = Exception("GET Exception")
        with pytest.raises(DBError):
            sf.download_from_internal("@~/internal", "/some/file")


@mock.patch("locopy.snowflake.PurePath", new=PureWindowsPath)
@mock.patch("locopy.s3.Session")
def test_download_from_internal_windows(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.download_from_internal("@~/internal", r"C:\some\file")
        sf.conn.cursor.return_value.execute.assert_called_with(
            "GET @~/internal 'file://C:/some/file' PARALLEL=10", ()
        )


@pytest.mark.parametrize(
    "file_type, format_options, copy_options, expected",
    [
        ("csv", None, None, "(TYPE='csv' FIELD_DELIMITER='|' SKIP_HEADER=0) "),
        (
            "csv",
            ["FIELD_DELIMITER=','", "SKIP_HEADER=1"],
            None,
            "(TYPE='csv' FIELD_DELIMITER=',' SKIP_HEADER=1) ",
        ),
        (
            "csv",
            ["FIELD_DELIMITER='|'", "SKIP_HEADER=0", "a=1", "b=2"],
            ["c=3", "d=4"],
            "(TYPE='csv' FIELD_DELIMITER='|' SKIP_HEADER=0 a=1 b=2) c=3 d=4",
        ),
        ("parquet", None, None, "(TYPE='parquet' ) "),
        (
            "parquet",
            ["BINARY_AS_TEXT=FALSE"],
            ["c=3", "d=4"],
            "(TYPE='parquet' BINARY_AS_TEXT=FALSE) c=3 d=4",
        ),
        ("json", None, None, "(TYPE='json' ) "),
        (
            "json",
            ["COMPRESSION=GZIP"],
            ["c=3", "d=4"],
            "(TYPE='json' COMPRESSION=GZIP) c=3 d=4",
        ),
    ],
)
@mock.patch("locopy.s3.Session")
def test_copy_file_type(
    mock_session, file_type, format_options, copy_options, expected, sf_credentials
):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.copy(
            "table_name",
            "@~/stage",
            file_type=file_type,
            format_options=format_options,
            copy_options=copy_options,
        )
        sf.conn.cursor.return_value.execute.assert_called_with(
            f"COPY INTO table_name FROM '@~/stage' FILE_FORMAT = {expected}",
            (),
        )


@pytest.mark.parametrize(
    "file_format_name, copy_options, expected",
    [
        ("my_csv_format", None, "(FORMAT_NAME='my_csv_format') "),
        (
            "my_csv_format",
            ["c=3", "d=4"],
            "(FORMAT_NAME='my_csv_format') c=3 d=4",
        ),
        ("my_parquet_format", None, "(FORMAT_NAME='my_parquet_format') "),
        (
            "my_parquet_format",
            ["c=3", "d=4"],
            "(FORMAT_NAME='my_parquet_format') c=3 d=4",
        ),
        ("my_json_format", None, "(FORMAT_NAME='my_json_format') "),
        (
            "my_json_format",
            ["c=3", "d=4"],
            "(FORMAT_NAME='my_json_format') c=3 d=4",
        ),
    ],
)
@mock.patch("locopy.s3.Session")
def test_copy_file_format_name(
    mock_session, file_format_name, copy_options, expected, sf_credentials
):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.copy(
            "table_name",
            "@~/stage",
            file_format_name=file_format_name,
            copy_options=copy_options,
        )
        sf.conn.cursor.return_value.execute.assert_called_with(
            f"COPY INTO table_name FROM '@~/stage' FILE_FORMAT = {expected}",
            (),
        )


@mock.patch("locopy.s3.Session")
def test_copy_exception(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        with pytest.raises(ValueError):
            sf.copy("table_name", "@~/stage", file_type="unknown")

        # exception
        sf.conn.cursor.return_value.execute.side_effect = Exception("COPY Exception")
        with pytest.raises(DBError):
            sf.copy("table_name", "@~/stage")

        sf.conn = None
        with pytest.raises(DBError):
            sf.copy("table_name", "@~/stage")


@pytest.mark.parametrize(
    "file_type, format_options, header, copy_options, expected",
    [
        ("csv", None, False, None, "(TYPE='csv' FIELD_DELIMITER='|') HEADER=False "),
        (
            "csv",
            ["FIELD_DELIMITER=','"],
            True,
            None,
            "(TYPE='csv' FIELD_DELIMITER=',') HEADER=True ",
        ),
        (
            "csv",
            ["FIELD_DELIMITER=','", "a=1", "b=2"],
            True,
            ["c=3", "d=4"],
            "(TYPE='csv' FIELD_DELIMITER=',' a=1 b=2) HEADER=True c=3 d=4",
        ),
        ("parquet", None, False, None, "(TYPE='parquet' ) HEADER=False "),
        (
            "parquet",
            ["SNAPPY_COMPRESSION=FALSE"],
            False,
            ["c=3", "d=4"],
            "(TYPE='parquet' SNAPPY_COMPRESSION=FALSE) HEADER=False c=3 d=4",
        ),
        ("json", None, False, None, "(TYPE='json' ) HEADER=False "),
        (
            "json",
            ["COMPRESSION=GZIP"],
            False,
            ["c=3", "d=4"],
            "(TYPE='json' COMPRESSION=GZIP) HEADER=False c=3 d=4",
        ),
    ],
)
@mock.patch("locopy.s3.Session")
def test_unload_file_type(
    mock_session,
    file_type,
    format_options,
    header,
    copy_options,
    expected,
    sf_credentials,
):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.unload(
            "@~/stage",
            "table_name",
            file_type=file_type,
            format_options=format_options,
            header=header,
            copy_options=copy_options,
        )
        sf.conn.cursor.return_value.execute.assert_called_with(
            f"COPY INTO @~/stage FROM table_name FILE_FORMAT = {expected}",
            (),
        )


@pytest.mark.parametrize(
    "file_format_name, header, copy_options, expected",
    [
        ("my_csv_format", False, None, "(FORMAT_NAME='my_csv_format') HEADER=False "),
        (
            "my_csv_format",
            True,
            ["c=3", "d=4"],
            "(FORMAT_NAME='my_csv_format') HEADER=True c=3 d=4",
        ),
        (
            "my_parquet_format",
            False,
            None,
            "(FORMAT_NAME='my_parquet_format') HEADER=False ",
        ),
        (
            "my_parquet_format",
            False,
            ["c=3", "d=4"],
            "(FORMAT_NAME='my_parquet_format') HEADER=False c=3 d=4",
        ),
        ("my_json_format", False, None, "(FORMAT_NAME='my_json_format') HEADER=False "),
        (
            "my_json_format",
            False,
            ["c=3", "d=4"],
            "(FORMAT_NAME='my_json_format') HEADER=False c=3 d=4",
        ),
    ],
)
@mock.patch("locopy.s3.Session")
def test_unload_file_format_name(
    mock_session,
    file_format_name,
    header,
    copy_options,
    expected,
    sf_credentials,
):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.unload(
            "@~/stage",
            "table_name",
            file_format_name=file_format_name,
            header=header,
            copy_options=copy_options,
        )
        sf.conn.cursor.return_value.execute.assert_called_with(
            f"COPY INTO @~/stage FROM table_name FILE_FORMAT = {expected}",
            (),
        )


@mock.patch("locopy.s3.Session")
def test_unload_exception(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        with pytest.raises(ValueError):
            sf.unload("table_name", "@~/stage", file_type="unknown")

        # exception
        sf.conn.cursor.return_value.execute.side_effect = Exception("UNLOAD Exception")
        with pytest.raises(DBError):
            sf.unload("@~/stage", "table_name")

        sf.conn = None
        with pytest.raises(DBError):
            sf.unload("@~/stage", "table_name")


@mock.patch("locopy.s3.Session")
def test_to_pandas(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.cursor._query_result_format = "arrow"
        sf.to_dataframe()
        sf.conn.cursor.return_value.fetch_pandas_all.assert_called_with()

        sf.cursor._query_result_format = "json"
        sf.to_dataframe()
        sf.conn.cursor.return_value.fetchall.assert_called_with()

        sf.to_dataframe(size=5)
        sf.conn.cursor.return_value.fetchmany.assert_called_with(5)


@mock.patch("locopy.s3.Session")
def test_to_polars(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.cursor._query_result_format = "arrow"
        sf.conn.cursor.return_value.fetch_arrow_all.return_value = pa.table(
            {"a": [1, 2, 3], "b": [4, 5, 6]}
        )
        polars_df = sf.to_dataframe(df_type="polars")
        expected_df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        assert_frame_equal(polars_df, expected_df)

        sf.cursor._query_result_format = "json"
        sf.to_dataframe(df_type="polars")
        sf.conn.cursor.return_value.fetchall.assert_called_with()

        sf.to_dataframe(df_type="polars", size=5)
        sf.conn.cursor.return_value.fetchmany.assert_called_with(5)


@mock.patch("locopy.s3.Session")
def test_to_dataframe_error(mock_session, sf_credentials):
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.cursor._query_result_format = "arrow"
        sf.conn.cursor.return_value.fetch_arrow_all.return_value = pa.table(
            {"a": [1, 2, 3], "b": [4, 5, 6]}
        )
        with pytest.raises(ValueError):
            polars_df = sf.to_dataframe(df_type="invalid")


@mock.patch("locopy.s3.Session")
def test_insert_pd_dataframe_to_table(mock_session, sf_credentials):
    import pandas as pd

    test_df = pd.read_csv(os.path.join(CURR_DIR, "data", "mock_dataframe.txt"), sep=",")
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.insert_dataframe_to_table(test_df, "database.schema.test")
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b,c) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )

        sf.insert_dataframe_to_table(test_df, "database.schema.test", create=True)
        sf.conn.cursor.return_value.execute.assert_any_call(
            "CREATE TABLE database.schema.test (a int,b varchar,c date)", ()
        )
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b,c) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )

        sf.insert_dataframe_to_table(
            test_df, "database.schema.test", columns=["a", "b"]
        )

        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b) VALUES (%s,%s)",
            [("1", "x"), ("2", "y")],
        )

        sf.insert_dataframe_to_table(
            test_df,
            "database.schema.test",
            create=True,
            metadata=OrderedDict(
                [("col1", "int"), ("col2", "varchar"), ("col3", "date")]
            ),
        )

        sf.conn.cursor.return_value.execute.assert_any_call(
            "CREATE TABLE database.schema.test (col1 int,col2 varchar,col3 date)",
            (),
        )
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (col1,col2,col3) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )

        sf.insert_dataframe_to_table(
            test_df,
            "database.schema.test",
            create=False,
            metadata=OrderedDict(
                [("col1", "int"), ("col2", "varchar"), ("col3", "date")]
            ),
        )

        # mock_session.warn.assert_called_with('Metadata will not be used because create is set to False.')
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b,c) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )


@mock.patch("locopy.s3.Session")
def test_insert_pl_dataframe_to_table(mock_session, sf_credentials):
    import polars as pl

    test_df = pl.read_csv(
        os.path.join(CURR_DIR, "data", "mock_dataframe.txt"), separator=","
    )
    with (
        mock.patch("snowflake.connector.connect") as mock_connect,
        Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf,
    ):
        sf.insert_dataframe_to_table(test_df, "database.schema.test")
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b,c) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )

        sf.insert_dataframe_to_table(test_df, "database.schema.test", create=True)
        sf.conn.cursor.return_value.execute.assert_any_call(
            "CREATE TABLE database.schema.test (a int,b varchar,c date)", ()
        )
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b,c) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )

        sf.insert_dataframe_to_table(
            test_df, "database.schema.test", columns=["a", "b"]
        )

        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b) VALUES (%s,%s)",
            [("1", "x"), ("2", "y")],
        )

        sf.insert_dataframe_to_table(
            test_df,
            "database.schema.test",
            create=True,
            metadata=OrderedDict(
                [("col1", "int"), ("col2", "varchar"), ("col3", "date")]
            ),
        )

        sf.conn.cursor.return_value.execute.assert_any_call(
            "CREATE TABLE database.schema.test (col1 int,col2 varchar,col3 date)",
            (),
        )
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (col1,col2,col3) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )

        sf.insert_dataframe_to_table(
            test_df,
            "database.schema.test",
            create=False,
            metadata=OrderedDict(
                [("col1", "int"), ("col2", "varchar"), ("col3", "date")]
            ),
        )

        # mock_session.warn.assert_called_with('Metadata will not be used because create is set to False.')
        sf.conn.cursor.return_value.executemany.assert_called_with(
            "INSERT INTO database.schema.test (a,b,c) VALUES (%s,%s,%s)",
            [("1", "x", "2011-01-01"), ("2", "y", "2001-04-02")],
        )
