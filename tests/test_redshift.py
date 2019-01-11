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
import pg8000, psycopg2
import locopy

from locopy import Redshift
from unittest import mock
from locopy.errors import CredentialsError, DBError


PROFILE = "test"
GOOD_CONFIG_YAML = """
host: host
port: port
database: database
user: user
password: password"""

DBAPIS = [pg8000, psycopg2]


def test_add_default_copy_options():
    assert locopy.redshift.add_default_copy_options() == [
        "DATEFORMAT 'auto'",
        "COMPUPDATE ON",
        "TRUNCATECOLUMNS",
    ]
    assert locopy.redshift.add_default_copy_options(["DATEFORMAT 'other'", "NULL AS 'blah'"]) == [
        "DATEFORMAT 'other'",
        "NULL AS 'blah'",
        "COMPUPDATE ON",
        "TRUNCATECOLUMNS",
    ]


def test_combine_copy_options():
    assert locopy.redshift.combine_copy_options(locopy.redshift.add_default_copy_options()) == (
        "DATEFORMAT 'auto' COMPUPDATE " "ON TRUNCATECOLUMNS"
    )


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_constructor(mock_session, credentials, dbapi):
    r = Redshift(profile=PROFILE, dbapi=dbapi, **credentials)
    mock_session.assert_called_with(profile_name=PROFILE)
    assert r.profile == PROFILE
    assert r.kms_key == None
    assert r.connection["host"] == "host"
    assert r.connection["port"] == "port"
    assert r.connection["database"] == "database"
    assert r.connection["user"] == "user"
    assert r.connection["password"] == "password"


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
@mock.patch("locopy.s3.Session")
def test_constructor_yaml(mock_session, dbapi):
    r = Redshift(profile=PROFILE, dbapi=dbapi, config_yaml="some_config.yml")
    mock_session.assert_called_with(profile_name=PROFILE)
    assert r.profile == PROFILE
    assert r.kms_key == None
    assert r.connection["host"] == "host"
    assert r.connection["port"] == "port"
    assert r.connection["database"] == "database"
    assert r.connection["user"] == "user"
    assert r.connection["password"] == "password"


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_redshift_connect(mock_session, credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = Redshift(profile=PROFILE, dbapi=dbapi, **credentials)
        r._connect()

        if dbapi.__name__ == "pg8000":
            mock_connect.assert_called_with(
                host="host",
                user="user",
                port="port",
                password="password",
                database="database",
                ssl=True,
            )
        else:
            mock_connect.assert_called_with(
                host="host",
                user="user",
                port="port",
                password="password",
                database="database",
                sslmode="require",
            )
        r.conn.cursor.assert_called_with()

        # side effect exception
        mock_connect.side_effect = Exception("Connect Exception")
        with pytest.raises(DBError):
            r._connect()


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.utility.os.remove")
@mock.patch("locopy.redshift.Redshift.copy")
@mock.patch("locopy.redshift.Redshift.upload_to_s3")
@mock.patch("locopy.redshift.Redshift.delete_from_s3")
@mock.patch("locopy.s3.Session")
@mock.patch("locopy.redshift.compress_file_list")
@mock.patch("locopy.redshift.split_file")
def test_load_and_copy(
    mock_split_file,
    mock_compress_file_list,
    mock_session,
    mock_s3_delete,
    mock_s3_upload,
    mock_rs_copy,
    mock_remove,
    credentials,
    dbapi,
):
    def reset_mocks():
        mock_split_file.reset_mock()
        mock_compress_file_list.reset_mock()
        mock_s3_upload.reset_mock()
        mock_s3_delete.reset_mock()
        mock_rs_copy.reset_mock()
        mock_remove.reset_mock()

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = Redshift(dbapi=dbapi, **credentials)
        r._connect()

        expected_calls_no_folder = [
            mock.call("/path/local_file.0", "s3_bucket", "local_file.0"),
            mock.call("/path/local_file.1", "s3_bucket", "local_file.1"),
            mock.call("/path/local_file.2", "s3_bucket", "local_file.2"),
        ]

        expected_calls_no_folder_gzip = [
            mock.call("/path/local_file.0.gz", "s3_bucket", "local_file.0.gz"),
            mock.call("/path/local_file.1.gz", "s3_bucket", "local_file.1.gz"),
            mock.call("/path/local_file.2.gz", "s3_bucket", "local_file.2.gz"),
        ]

        expected_calls_folder = [
            mock.call("/path/local_file.0", "s3_bucket", "test/local_file.0"),
            mock.call("/path/local_file.1", "s3_bucket", "test/local_file.1"),
            mock.call("/path/local_file.2", "s3_bucket", "test/local_file.2"),
        ]

        expected_calls_folder_gzip = [
            mock.call("/path/local_file.0.gz", "s3_bucket", "test/local_file.0.gz"),
            mock.call("/path/local_file.1.gz", "s3_bucket", "test/local_file.1.gz"),
            mock.call("/path/local_file.2.gz", "s3_bucket", "test/local_file.2.gz"),
        ]

        mock_split_file.return_value = ["/path/local_file.txt"]
        mock_compress_file_list.return_value = ["/path/local_file.txt.gz"]
        r.load_and_copy("/path/local_file.txt", "s3_bucket", "table_name", delim="|")

        # assert
        assert mock_split_file.called
        mock_compress_file_list.assert_called_with(["/path/local_file.txt"])
        # mock_remove.assert_called_with("/path/local_file.txt")
        mock_s3_upload.assert_called_with(
            "/path/local_file.txt.gz", "s3_bucket", "local_file.txt.gz"
        )
        mock_rs_copy.assert_called_with(
            "table_name", "s3://s3_bucket/local_file", "|", copy_options=["GZIP"]
        )
        assert not mock_s3_delete.called, "Only delete when explicit"

        reset_mocks()
        mock_split_file.return_value = [
            "/path/local_file.0",
            "/path/local_file.1",
            "/path/local_file.2",
        ]
        mock_compress_file_list.return_value = [
            "/path/local_file.0.gz",
            "/path/local_file.1.gz",
            "/path/local_file.2.gz",
        ]
        r.load_and_copy(
            "/path/local_file",
            "s3_bucket",
            "table_name",
            delim="|",
            copy_options=["SOME OPTION"],
            splits=3,
            delete_s3_after=True,
        )

        # assert
        mock_split_file.assert_called_with("/path/local_file", "/path/local_file", splits=3)
        mock_compress_file_list.assert_called_with(
            ["/path/local_file.0", "/path/local_file.1", "/path/local_file.2"]
        )
        # mock_remove.assert_called_with("/path/local_file.2")
        mock_s3_upload.assert_has_calls(expected_calls_no_folder_gzip)
        mock_rs_copy.assert_called_with(
            "table_name", "s3://s3_bucket/local_file", "|", copy_options=["SOME OPTION", "GZIP"]
        )
        assert mock_s3_delete.called_with("s3_bucket", "local_file.0.gz")
        assert mock_s3_delete.called_with("s3_bucket", "local_file.1.gz")
        assert mock_s3_delete.called_with("s3_bucket", "local_file.2.gz")

        reset_mocks()
        mock_split_file.return_value = ["/path/local_file"]
        mock_compress_file_list.return_value = ["/path/local_file.gz"]
        r.load_and_copy(
            "/path/local_file",
            "s3_bucket",
            "table_name",
            delim=",",
            copy_options=["SOME OPTION"],
            compress=False,
        )
        # assert
        assert mock_split_file.called
        assert not mock_compress_file_list.called
        # assert not mock_remove.called
        mock_s3_upload.assert_called_with("/path/local_file", "s3_bucket", "local_file")
        mock_rs_copy.assert_called_with(
            "table_name", "s3://s3_bucket/local_file", ",", copy_options=["SOME OPTION"]
        )
        assert not mock_s3_delete.called, "Only delete when explicit"

        reset_mocks()
        mock_split_file.return_value = [
            "/path/local_file.0",
            "/path/local_file.1",
            "/path/local_file.2",
        ]
        r.load_and_copy(
            "/path/local_file",
            "s3_bucket",
            "table_name",
            delim="|",
            copy_options=["SOME OPTION"],
            splits=3,
            compress=False,
        )
        # assert
        mock_split_file.assert_called_with("/path/local_file", "/path/local_file", splits=3)
        assert not mock_compress_file_list.called
        # assert not mock_remove.called
        mock_s3_upload.assert_has_calls(expected_calls_no_folder)
        mock_rs_copy.assert_called_with(
            "table_name", "s3://s3_bucket/local_file", "|", copy_options=["SOME OPTION"]
        )
        assert not mock_s3_delete.called

        # with a s3_folder included and no splits
        reset_mocks()
        mock_split_file.return_value = ["/path/local_file.txt"]
        r.load_and_copy(
            "/path/local_file.txt",
            "s3_bucket",
            "table_name",
            delim="|",
            copy_options=["SOME OPTION"],
            compress=False,
            s3_folder="test",
        )
        # assert
        assert mock_split_file.called
        assert not mock_compress_file_list.called
        # assert not mock_remove.called
        mock_s3_upload.assert_called_with(
            "/path/local_file.txt", "s3_bucket", "test/local_file.txt"
        )
        mock_rs_copy.assert_called_with(
            "table_name", "s3://s3_bucket/test/local_file", "|", copy_options=["SOME OPTION"]
        )
        assert not mock_s3_delete.called

        # with a s3_folder included and splits
        reset_mocks()
        mock_split_file.return_value = [
            "/path/local_file.0",
            "/path/local_file.1",
            "/path/local_file.2",
        ]

        r.load_and_copy(
            "/path/local_file",
            "s3_bucket",
            "table_name",
            delim="|",
            copy_options=["SOME OPTION"],
            splits=3,
            compress=False,
            s3_folder="test",
            delete_s3_after=True,
        )
        # assert
        mock_split_file.assert_called_with("/path/local_file", "/path/local_file", splits=3)
        assert not mock_compress_file_list.called
        # assert not mock_remove.called
        mock_s3_upload.assert_has_calls(expected_calls_folder)
        mock_rs_copy.assert_called_with(
            "table_name", "s3://s3_bucket/test/local_file", "|", copy_options=["SOME OPTION"]
        )
        assert mock_s3_delete.called_with("s3_bucket", "test/local_file.0")
        assert mock_s3_delete.called_with("s3_bucket", "test/local_file.1")
        assert mock_s3_delete.called_with("s3_bucket", "test/local_file.2")

        # with a s3_folder included , splits, and gzip
        reset_mocks()
        mock_split_file.return_value = [
            "/path/local_file.0",
            "/path/local_file.1",
            "/path/local_file.2",
        ]
        mock_compress_file_list.return_value = [
            "/path/local_file.0.gz",
            "/path/local_file.1.gz",
            "/path/local_file.2.gz",
        ]
        r.load_and_copy(
            "/path/local_file",
            "s3_bucket",
            "table_name",
            delim="|",
            copy_options=["SOME OPTION"],
            splits=3,
            s3_folder="test",
        )
        # assert
        mock_split_file.assert_called_with("/path/local_file", "/path/local_file", splits=3)
        assert mock_compress_file_list.called
        # assert mock_remove.called
        mock_s3_upload.assert_has_calls(expected_calls_folder_gzip)
        mock_rs_copy.assert_called_with(
            "table_name",
            "s3://s3_bucket/test/local_file",
            "|",
            copy_options=["SOME OPTION", "GZIP"],
        )
        assert not mock_s3_delete.called


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_redshiftcopy(mock_session, credentials, dbapi):

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        r.copy("table", "s3bucket")
        assert mock_connect.return_value.cursor.return_value.execute.called
        (
            mock_connect.return_value.cursor.return_value.execute.assert_called_with(
                "COPY table FROM 's3bucket' CREDENTIALS "
                "'aws_access_key_id={0};aws_secret_access_key={1};token={2}' "
                "DELIMITER '|' DATEFORMAT 'auto' COMPUPDATE ON "
                "TRUNCATECOLUMNS;".format(
                    r.session.get_credentials().access_key,
                    r.session.get_credentials().secret_key,
                    r.session.get_credentials().token,
                ),
                None,
            )
        )

        # tab delim
        r.copy("table", "s3bucket", delim="\t")
        assert mock_connect.return_value.cursor.return_value.execute.called
        (
            mock_connect.return_value.cursor.return_value.execute.assert_called_with(
                "COPY table FROM 's3bucket' CREDENTIALS "
                "'aws_access_key_id={0};aws_secret_access_key={1};token={2}' "
                "DELIMITER '\t' DATEFORMAT 'auto' COMPUPDATE ON "
                "TRUNCATECOLUMNS;".format(
                    r.session.get_credentials().access_key,
                    r.session.get_credentials().secret_key,
                    r.session.get_credentials().token,
                ),
                None,
            )
        )


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
@mock.patch("locopy.database.Database._is_connected")
def test_redshiftcopy_exception(mock_connected, mock_session, credentials, dbapi):

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        mock_connected.return_value = False

        with pytest.raises(DBError):
            r.copy("table", "s3bucket")

        mock_connected.return_value = True
        (mock_connect.return_value.cursor.return_value.execute.side_effect) = Exception(
            "COPY Exception"
        )
        with pytest.raises(DBError):
            r.copy("table", "s3bucket")


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.redshift.concatenate_files")
@mock.patch("locopy.s3.S3.delete_list_from_s3")
@mock.patch("locopy.redshift.write_file")
@mock.patch("locopy.s3.S3.download_list_from_s3")
@mock.patch("locopy.redshift.Redshift._get_column_names")
@mock.patch("locopy.redshift.Redshift._unload_generated_files")
@mock.patch("locopy.redshift.Redshift.unload")
@mock.patch("locopy.s3.S3._generate_unload_path")
@mock.patch("locopy.s3.Session")
def test_unload_and_copy(
    mock_session,
    mock_generate_unload_path,
    mock_unload,
    mock_unload_generated_files,
    mock_get_col_names,
    mock_download_list_from_s3,
    mock_write,
    mock_delete_list_from_s3,
    mock_concat,
    credentials,
    dbapi,
):
    def reset_mocks():
        mock_session.reset_mock()
        mock_generate_unload_path.reset_mock()
        mock_unload_generated_files.reset_mock()
        mock_get_col_names.reset_mock()
        mock_write.reset_mock()
        mock_download_list_from_s3.reset_mock()
        mock_delete_list_from_s3.reset_mock()
        mock_concat.reset_mock()

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.Redshift(dbapi=dbapi, **credentials)

        ##
        ## Test 1: check that basic export pipeline functions are called
        mock_unload_generated_files.return_value = ["dummy_file"]
        mock_download_list_from_s3.return_value = ["s3.file"]
        mock_get_col_names.return_value = ["dummy_col_name"]
        mock_generate_unload_path.return_value = "dummy_s3_path"

        ## ensure nothing is returned when read=False
        r.unload_and_copy(
            query="query",
            s3_bucket="s3_bucket",
            s3_folder=None,
            export_path=False,
            delimiter=",",
            delete_s3_after=False,
            parallel_off=False,
        )

        assert mock_unload_generated_files.called
        assert not mock_write.called, "write_file should only be called " "if export_path != False"
        mock_generate_unload_path.assert_called_with("s3_bucket", None)
        mock_get_col_names.assert_called_with("query")
        mock_unload.assert_called_with(
            query="query", s3path="dummy_s3_path", unload_options=["DELIMITER ','"]
        )
        assert not mock_delete_list_from_s3.called

        ##
        ## Test 2: different delimiter
        reset_mocks()
        mock_unload_generated_files.return_value = ["dummy_file"]
        mock_download_list_from_s3.return_value = ["s3.file"]
        mock_get_col_names.return_value = ["dummy_col_name"]
        mock_generate_unload_path.return_value = "dummy_s3_path"
        r.unload_and_copy(
            query="query",
            s3_bucket="s3_bucket",
            s3_folder=None,
            export_path=False,
            delimiter="|",
            delete_s3_after=False,
            parallel_off=True,
        )

        ## check that unload options are modified based on supplied args
        mock_unload.assert_called_with(
            query="query", s3path="dummy_s3_path", unload_options=["DELIMITER '|'", "PARALLEL OFF"]
        )
        assert not mock_delete_list_from_s3.called

        ##
        ## Test 3: ensure exception is raised when no column names are retrieved
        reset_mocks()
        mock_unload_generated_files.return_value = ["dummy_file"]
        mock_generate_unload_path.return_value = "dummy_s3_path"
        mock_get_col_names.return_value = None
        with pytest.raises(Exception):
            r.unload_and_copy("query", "s3_bucket", None)

        ##
        ## Test 4: ensure exception is raised when no files are returned
        reset_mocks()
        mock_generate_unload_path.return_value = "dummy_s3_path"
        mock_get_col_names.return_value = ["dummy_col_name"]
        mock_unload_generated_files.return_value = None
        with pytest.raises(Exception):
            r.unload_and_copy("query", "s3_bucket", None)

        ##
        ## Test 5: ensure file writing is initiated when export_path is supplied
        reset_mocks()
        mock_get_col_names.return_value = ["dummy_col_name"]
        mock_download_list_from_s3.return_value = ["s3.file"]
        mock_generate_unload_path.return_value = "dummy_s3_path"
        mock_unload_generated_files.return_value = ["/dummy_file"]
        r.unload_and_copy(
            query="query",
            s3_bucket="s3_bucket",
            s3_folder=None,
            export_path="my_output.csv",
            delimiter=",",
            delete_s3_after=True,
            parallel_off=False,
        )
        mock_concat.assert_called_with(mock_download_list_from_s3.return_value, "my_output.csv")
        assert mock_write.called
        assert mock_delete_list_from_s3.called_with("s3_bucket", "my_output.csv")


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_unload_generated_files(mock_session, credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        r._unload_generated_files()
        assert r._unload_generated_files() == None

        mock_connect.return_value.cursor.return_value.fetchall.return_value = [
            ["File1 "],
            ["File2 "],
        ]
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        r._unload_generated_files()
        assert r._unload_generated_files() == ["File1", "File2"]

        mock_connect.return_value.cursor.return_value.execute.side_effect = Exception()
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        with pytest.raises(Exception):
            r._unload_generated_files()


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_get_column_names(mock_session, credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        assert r._get_column_names("query") == None
        sql = "SELECT * FROM (query) WHERE 1 = 0"
        assert mock_connect.return_value.cursor.return_value.execute.called_with(sql, None)

        mock_connect.return_value.cursor.return_value.description = [["COL1 "], ["COL2 "]]
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        assert r._get_column_names("query") == ["COL1", "COL2"]

        mock_connect.return_value.cursor.return_value.execute.side_effect = Exception()
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        with pytest.raises(Exception):
            r._get_column_names("query")


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def testunload(mock_session, credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        r.unload("query", "path")
        assert mock_connect.return_value.cursor.return_value.execute.called


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def testunload_no_connection(mock_session, credentials, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        with pytest.raises(Exception):
            r.unload("query", "path")

        mock_connect.return_value.cursor.return_value.execute.side_effect = Exception()
        r = locopy.Redshift(dbapi=dbapi, **credentials)
        r._connect()
        with pytest.raises(Exception):
            r.unload("query", "path")
