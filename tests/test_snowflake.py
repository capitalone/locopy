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


import pytest
import snowflake.connector
import locopy

from pathlib import PureWindowsPath
from locopy import Snowflake
from unittest import mock
from locopy.errors import CredentialsError, DBError


PROFILE = "test"
KMS = "kms_test"

GOOD_CONFIG_YAML = """
account: account
warehouse: warehouse
database: database
user: user
password: password"""

DBAPIS = snowflake.connector


def test_combine_options():
    assert locopy.snowflake.combine_options(None) == ""
    assert locopy.snowflake.combine_options(["a", "b", "c"]) == "a b c"
    assert locopy.snowflake.combine_options(["a=1", "b=2", "c=3"]) == "a=1 b=2 c=3"
    assert locopy.snowflake.combine_options([""]) == ""
    with pytest.raises(TypeError):
        assert locopy.snowflake.combine_options([1])


@mock.patch("locopy.s3.Session")
def test_constructor(mock_session, sf_credentials):
    sf = Snowflake(profile=PROFILE, kms_key=KMS, dbapi=DBAPIS, **sf_credentials)
    mock_session.assert_called_with(profile_name=PROFILE)
    assert sf.profile == PROFILE
    assert sf.kms_key == KMS
    assert sf.connection["account"] == "account"
    assert sf.connection["warehouse"] == "warehouse"
    assert sf.connection["database"] == "database"
    assert sf.connection["user"] == "user"
    assert sf.connection["password"] == "password"


@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
@mock.patch("locopy.s3.Session")
def test_constructor_yaml(mock_session):
    sf = Snowflake(profile=PROFILE, kms_key=KMS, dbapi=DBAPIS, config_yaml="some_config.yml")
    mock_session.assert_called_with(profile_name=PROFILE)
    assert sf.profile == PROFILE
    assert sf.kms_key == KMS
    assert sf.connection["account"] == "account"
    assert sf.connection["warehouse"] == "warehouse"
    assert sf.connection["database"] == "database"
    assert sf.connection["user"] == "user"
    assert sf.connection["password"] == "password"


@mock.patch("locopy.s3.Session")
def test_connect(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        sf = Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials)
        sf._connect()

        mock_connect.assert_called_with(
            account="account",
            user="user",
            warehouse="warehouse",
            password="password",
            database="database",
        )
        sf.conn.cursor.assert_called_with()
        sf.conn.cursor.return_value.execute.assert_any_call("USE WAREHOUSE warehouse", None)
        sf.conn.cursor.return_value.execute.assert_any_call("USE DATABASE database", None)

        # side effect exception
        mock_connect.side_effect = Exception("Connect Exception")
        with pytest.raises(DBError):
            sf._connect()


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
            )
            sf.conn.cursor.assert_called_with()
            sf.conn.cursor.return_value.execute.assert_any_call("USE WAREHOUSE warehouse", None)
            sf.conn.cursor.return_value.execute.assert_any_call("USE DATABASE database", None)

        mock_connect.side_effect = Exception("Connect Exception")
        with pytest.raises(DBError):
            with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:
                sf.cursor


@mock.patch("locopy.s3.Session")
def test_upload_to_internal(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:
            sf.upload_to_internal("/some/file", "@~/internal")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "PUT 'file:///some/file' @~/internal PARALLEL=4 AUTO_COMPRESS=True", None
            )

            sf.upload_to_internal("/some/file", "@~/internal", parallel=99, auto_compress=False)
            sf.conn.cursor.return_value.execute.assert_called_with(
                "PUT 'file:///some/file' @~/internal PARALLEL=99 AUTO_COMPRESS=False", None
            )

            # exception
            sf.conn.cursor.return_value.execute.side_effect = Exception("PUT Exception")
            with pytest.raises(DBError):
                sf.upload_to_internal("/some/file", "@~/internal")


@mock.patch("locopy.snowflake.PurePath", new=PureWindowsPath)
@mock.patch("locopy.s3.Session")
def test_upload_to_internal_windows(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

            sf.upload_to_internal(r"C:\some\file", "@~/internal")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "PUT 'file://C:/some/file' @~/internal PARALLEL=4 AUTO_COMPRESS=True", None
            )


@mock.patch("locopy.s3.Session")
def test_download_from_internal(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:
            sf.download_from_internal("@~/internal", "/some/file")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "GET @~/internal 'file:///some/file' PARALLEL=10", None
            )

            sf.download_from_internal("@~/internal", "/some/file", parallel=99)
            sf.conn.cursor.return_value.execute.assert_called_with(
                "GET @~/internal 'file:///some/file' PARALLEL=99", None
            )

            # exception
            sf.conn.cursor.return_value.execute.side_effect = Exception("GET Exception")
            with pytest.raises(DBError):
                sf.download_from_internal("@~/internal", "/some/file")


@mock.patch("locopy.snowflake.PurePath", new=PureWindowsPath)
@mock.patch("locopy.s3.Session")
def test_download_from_internal_windows(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

            sf.download_from_internal("@~/internal", r"C:\some\file")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "GET @~/internal 'file://C:/some/file' PARALLEL=10", None
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
        ("json", ["COMPRESSION=GZIP"], ["c=3", "d=4"], "(TYPE='json' COMPRESSION=GZIP) c=3 d=4"),
    ],
)
@mock.patch("locopy.s3.Session")
def test_copy(mock_session, file_type, format_options, copy_options, expected, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

            sf.copy(
                "table_name",
                "@~/stage",
                file_type=file_type,
                format_options=format_options,
                copy_options=copy_options,
            )
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO table_name FROM '@~/stage' FILE_FORMAT = {0}".format(expected), None
            )


@mock.patch("locopy.s3.Session")
def test_copy_exception(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

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
def test_unload(
    mock_session, file_type, format_options, header, copy_options, expected, sf_credentials
):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

            sf.unload(
                "@~/stage",
                "table_name",
                file_type=file_type,
                format_options=format_options,
                header=header,
                copy_options=copy_options,
            )
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO @~/stage FROM table_name FILE_FORMAT = {0}".format(expected), None
            )


@mock.patch("locopy.s3.Session")
def test_unload_exception(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

            with pytest.raises(ValueError):
                sf.unload("table_name", "@~/stage", file_type="unknown")

            # exception
            sf.conn.cursor.return_value.execute.side_effect = Exception("UNLOAD Exception")
            with pytest.raises(DBError):
                sf.unload("@~/stage", "table_name")

            sf.conn = None
            with pytest.raises(DBError):
                sf.unload("@~/stage", "table_name")
