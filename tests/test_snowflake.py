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
                "PUT file:///some/file @~/internal PARALLEL=4 AUTO_COMPRESS=True", None
            )

            sf.upload_to_internal("/some/file", "@~/internal", parallel=99, auto_compress=False)
            sf.conn.cursor.return_value.execute.assert_called_with(
                "PUT file:///some/file @~/internal PARALLEL=99 AUTO_COMPRESS=False", None
            )

            sf.upload_to_internal("C:\some\file", "@~/internal")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "PUT file://C:\some\file @~/internal PARALLEL=4 AUTO_COMPRESS=True", None
            )

            # exception
            sf.conn.cursor.return_value.execute.side_effect = Exception("PUT Exception")
            with pytest.raises(DBError):
                sf.upload_to_internal("/some/file", "@~/internal")


@mock.patch("locopy.s3.Session")
def test_download_from_internal(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:
            sf.download_from_internal("@~/internal", "/some/file")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "GET @~/internal file:///some/file PARALLEL=10", None
            )

            sf.download_from_internal("@~/internal", "/some/file", parallel=99)
            sf.conn.cursor.return_value.execute.assert_called_with(
                "GET @~/internal file:///some/file PARALLEL=99", None
            )

            sf.download_from_internal("@~/internal", "C:\some\file")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "GET @~/internal file://C:\some\file PARALLEL=10", None
            )

            # exception
            sf.conn.cursor.return_value.execute.side_effect = Exception("GET Exception")
            with pytest.raises(DBError):
                sf.download_from_internal("@~/internal", "/some/file")


@mock.patch("locopy.s3.Session")
def test_copy(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

            sf.copy("table_name", "@~/stage")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO table_name FROM '@~/stage' FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER='|' SKIP_HEADER=0 ) ",
                None,
            )

            sf.copy("table_name", "@~/stage", delim=",", header=True)
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO table_name FROM '@~/stage' FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER=',' SKIP_HEADER=1 ) ",
                None,
            )

            sf.copy(
                "table_name", "@~/stage", format_options=["a=1", "b=2"], copy_options=["c=3", "d=4"]
            )
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO table_name FROM '@~/stage' FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER='|' SKIP_HEADER=0 a=1 b=2) c=3 d=4",
                None,
            )

            # exception
            sf.conn.cursor.return_value.execute.side_effect = Exception("COPY Exception")
            with pytest.raises(DBError):
                sf.copy("table_name", "@~/stage")

            sf.conn = None
            with pytest.raises(DBError):
                sf.copy("table_name", "@~/stage")


@mock.patch("locopy.s3.Session")
def test_unload(mock_session, sf_credentials):
    with mock.patch("snowflake.connector.connect") as mock_connect:
        with Snowflake(profile=PROFILE, dbapi=DBAPIS, **sf_credentials) as sf:

            sf.unload("@~/stage", "table_name")
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO @~/stage FROM table_name FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER='|' ) HEADER=False ",
                None,
            )

            sf.unload("@~/stage", "table_name", delim=",", header=True)
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO @~/stage FROM table_name FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER=',' ) HEADER=True ",
                None,
            )

            sf.unload(
                "@~/stage", "table_name", format_options=["a=1", "b=2"], copy_options=["c=3", "d=4"]
            )
            sf.conn.cursor.return_value.execute.assert_called_with(
                "COPY INTO @~/stage FROM table_name FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER='|' a=1 b=2) HEADER=False c=3 d=4",
                None,
            )

            # exception
            sf.conn.cursor.return_value.execute.side_effect = Exception("UNLOAD Exception")
            with pytest.raises(DBError):
                sf.unload("@~/stage", "table_name")

            sf.conn = None
            with pytest.raises(DBError):
                sf.unload("@~/stage", "table_name")
