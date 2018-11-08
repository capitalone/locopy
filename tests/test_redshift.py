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

from unittest import mock
from tests import RedshiftNoConnect
from locopy.errors import CredentialsError, ConnectionError, DisconnectionError, DBError


GOOD_CONFIG_YAML = """
host: host
port: 1234
dbname: db
user: id
password: pass"""


DBAPIS = [pg8000, psycopg2]


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_constructor(rs_creds, dbapi):
    r = RedshiftNoConnect(dbapi=dbapi, **rs_creds)
    assert r.host == "host"
    assert r.port == "port"
    assert r.dbname == "dbname"
    assert r.user == "user"
    assert r.password == "password"


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
def test_constructor_yaml(dbapi):
    r = RedshiftNoConnect(dbapi=dbapi, config_yaml="some_config.yml")
    assert r.host == "host"
    assert r.port == 1234
    assert r.dbname == "db"
    assert r.user == "id"
    assert r.password == "pass"


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_validate_fields(rs_bad_1, rs_bad_2, rs_bad_3, rs_bad_4, rs_bad_5, dbapi):
    with pytest.raises(CredentialsError):
        RedshiftNoConnect(dbapi=dbapi, **rs_bad_1)
    with pytest.raises(CredentialsError):
        RedshiftNoConnect(dbapi=dbapi, **rs_bad_2)
    with pytest.raises(CredentialsError):
        RedshiftNoConnect(dbapi=dbapi, **rs_bad_3)
    with pytest.raises(CredentialsError):
        RedshiftNoConnect(dbapi=dbapi, **rs_bad_4)
    with pytest.raises(CredentialsError):
        RedshiftNoConnect(dbapi=dbapi, **rs_bad_5)


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_get_redshift(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        locopy.Redshift(dbapi=dbapi, **rs_creds)

    if dbapi.__name__ == "pg8000":
        mock_connect.assert_called_with(
            host="host", user="user", port="port", password="password", database="dbname", ssl=True
        )
    else:
        mock_connect.assert_called_with(
            host="host",
            user="user",
            port="port",
            password="password",
            database="dbname",
            sslmode="require",
        )


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_redshift_connect(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test_redshift = locopy.Redshift(dbapi=dbapi, **rs_creds)

    if dbapi.__name__ == "pg8000":
        mock_connect.assert_called_with(
            host="host", user="user", port="port", password="password", database="dbname", ssl=True
        )
    else:
        mock_connect.assert_called_with(
            host="host",
            user="user",
            port="port",
            password="password",
            database="dbname",
            sslmode="require",
        )

    test_redshift.conn.cursor.assert_called_with()

    # side effect exception
    mock_connect.side_effect = Exception("Connect Exception")
    with pytest.raises(ConnectionError):
        test_redshift._connect()


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_redshift_disconnect(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test_redshift = locopy.Redshift(dbapi=dbapi, **rs_creds)
    test_redshift._disconnect()
    test_redshift.conn.close.assert_called_with()
    test_redshift.cursor.close.assert_called_with()

    # side effect exception
    test_redshift.conn.close.side_effect = Exception("Disconnect Exception")
    with pytest.raises(DisconnectionError):
        test_redshift._disconnect()


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_is_connected(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test_redshift = RedshiftNoConnect(dbapi=dbapi, **rs_creds)
    assert test_redshift._is_connected() is False

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test_redshift = locopy.Redshift(dbapi=dbapi, **rs_creds)
    assert test_redshift._is_connected() is True

    # throws exception in _is_connected
    test_redshift = RedshiftNoConnect(**rs_creds)
    del test_redshift.conn
    assert test_redshift._is_connected() is False


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_execute(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        with locopy.Redshift(dbapi=dbapi, **rs_creds) as test:
            test.execute("SELECT * FROM some_table")
            assert test.cursor.execute.called is True


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_execute_no_connection_exception(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test = locopy.Redshift(dbapi=dbapi, **rs_creds)
    test.conn = None
    test.cursor = None

    with pytest.raises(ConnectionError):
        test.execute("SELECT * FROM some_table")


@pytest.mark.parametrize("dbapi", DBAPIS)
def test_execute_sql_exception(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        with locopy.Redshift(dbapi=dbapi, **rs_creds) as test:
            test.cursor.execute.side_effect = Exception("SQL Exception")
            with pytest.raises(DBError):
                test.execute("SELECT * FROM some_table")


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("pandas.DataFrame")
def test_to_dataframe_all(mock_pandas, dbapi, rs_creds):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchall.return_value = [(1, 2), (2, 3), (3,)]
        with locopy.Redshift(dbapi=dbapi, **rs_creds) as test:
            test.execute("SELECT 'hello world' AS fld")
            df = test.to_dataframe()

    assert mock_connect.return_value.cursor.return_value.fetchall.called
    mock_pandas.assert_called_with(test.cursor.fetchall(), columns=[])


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("pandas.DataFrame")
def test_to_dataframe_custom_size(mock_pandas, dbapi, rs_creds):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchmany.return_value = [
            (1, 2),
            (2, 3),
            (3,),
        ]
        with locopy.Redshift(dbapi=dbapi, **rs_creds) as test:
            test.execute("SELECT 'hello world' AS fld")
            df = test.to_dataframe(size=5)

    mock_connect.return_value.cursor.return_value.fetchmany.assert_called_with(5)
    mock_pandas.assert_called_with(test.cursor.fetchmany(), columns=[])


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("pandas.DataFrame")
def test_to_dataframe_none(mock_pandas, dbapi, rs_creds):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchmany.return_value = []
        with locopy.Redshift(dbapi=dbapi, **rs_creds) as test:
            test.execute("SELECT 'hello world' WHERE 1=0")
            assert test.to_dataframe(size=5) is None
            mock_pandas.assert_not_called()


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_super_init(mock_session, rs_creds, dbapi):
    # Test that the super class (locopy) gets the right stuff
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        s = locopy.S3(dbapi=dbapi, **rs_creds)
    assert s.host == "host"
    assert s.port == "port"
    assert s.dbname == "dbname"
    assert s.user == "user"
    assert s.password == "password"

    if dbapi.__name__ == "pg8000":
        mock_connect.assert_called_with(
            host="host", user="user", port="port", password="password", database="dbname", ssl=True
        )
    else:
        mock_connect.assert_called_with(
            host="host",
            user="user",
            port="port",
            password="password",
            database="dbname",
            sslmode="require",
        )


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
@mock.patch("locopy.s3.Session")
def test_super_init_yaml(mock_session, dbapi):
    # Test that the super class (locopy) gets the right stuff with a yaml
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        s = locopy.S3(dbapi=dbapi, config_yaml="MY_YAML_FILE.yml")
    assert s.host == "host"
    assert s.port == 1234
    assert s.dbname == "db"
    assert s.user == "userid"
    assert s.password == "pass"

    if dbapi.__name__ == "pg8000":
        mock_connect.assert_called_with(
            host="host", user="userid", port=1234, password="pass", database="db", ssl=True
        )
    else:
        mock_connect.assert_called_with(
            host="host", user="userid", port=1234, password="pass", database="db", sslmode="require"
        )


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
@mock.patch("locopy.s3.Session")
def test_super_init_s3_only(mock_session, dbapi):
    s = locopy.S3(dbapi=dbapi, config_yaml="MY_YAML_FILE.yml", s3_only=True)
    assert hasattr(s, "host") == False
    assert hasattr(s, "port") == False
    assert hasattr(s, "dbname") == False
    assert hasattr(s, "user") == False
    assert hasattr(s, "password") == False
    assert s._is_connected() == False


@mock.patch("locopy.s3.Session.get_credentials")
def test_get_profile_tokens(mock_cred, aws_creds):
    r = MockS3()
    mock_cred.return_value = aws_creds
    cred_string = r._credentials_string()
    expected = "aws_access_key_id=access;" "aws_secret_access_key=secret;" "token=token"
    assert cred_string == expected


@mock.patch("locopy.s3.Session.get_credentials")
def test_get_profile_tokens_no_token(mock_cred):
    r = MockS3()
    mock_cred.return_value = Credentials("aws_access_key_id", "aws_secret_access_key")
    cred_string = r._credentials_string()
    expected = "aws_access_key_id=aws_access_key_id;" "aws_secret_access_key=aws_secret_access_key"
    assert cred_string == expected


@mock.patch("locopy.s3.Session.get_credentials")
def test_get_profile_tokens_exception(mock_cred):
    mock_cred.side_effect = Exception("Exception")
    with pytest.raises(Exception):
        MockS3()


@mock.patch("locopy.s3.Session.get_credentials")
def test_get_credentials_exception(mock_cred):
    mock_cred.reset_mock()
    mock_cred.return_value = None
    with pytest.raises(S3CredentialsError):
        MockS3()


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.os.remove")
@mock.patch("locopy.s3.S3._copy_to_redshift")
@mock.patch("locopy.s3.S3.upload_to_s3")
@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.Session")
@mock.patch("locopy.s3.compress_file")
@mock.patch("locopy.s3.split_file")
def test_run_copy(
    mock_split_file,
    mock_compress_file,
    mock_session,
    mock_s3_delete,
    mock_s3_upload,
    mock_rs_copy,
    mock_remove,
    rs_creds,
    dbapi,
):
    def reset_mocks():
        mock_split_file.reset_mock()
        mock_compress_file.reset_mock()
        mock_s3_upload.reset_mock()
        mock_s3_delete.reset_mock()
        mock_rs_copy.reset_mock()
        mock_remove.reset_mock()

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)

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

    r.run_copy(
        "/path/local_file.txt", "s3_bucket", "table_name", delim="|", copy_options=["SOME OPTION"]
    )

    # assert
    assert not mock_split_file.called
    mock_compress_file.assert_called_with("/path/local_file.txt", "/path/local_file.txt.gz")
    mock_remove.assert_called_with("/path/local_file.txt")
    mock_s3_upload.assert_called_with("/path/local_file.txt.gz", "s3_bucket", "local_file.txt.gz")
    mock_rs_copy.assert_called_with(
        "table_name", "s3://s3_bucket/local_file", "|", copy_options=["SOME OPTION", "GZIP"]
    )
    assert not mock_s3_delete.called, "Only delete when explicit"

    reset_mocks()
    mock_split_file.return_value = [
        "/path/local_file.0",
        "/path/local_file.1",
        "/path/local_file.2",
    ]
    r.run_copy(
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
    mock_compress_file.assert_called_with("/path/local_file.2", "/path/local_file.2.gz")
    mock_remove.assert_called_with("/path/local_file.2")
    mock_s3_upload.assert_has_calls(expected_calls_no_folder_gzip)
    mock_rs_copy.assert_called_with(
        "table_name", "s3://s3_bucket/local_file", "|", copy_options=["SOME OPTION", "GZIP"]
    )
    assert mock_s3_delete.called_with("s3_bucket", "local_file.0.gz")
    assert mock_s3_delete.called_with("s3_bucket", "local_file.1.gz")
    assert mock_s3_delete.called_with("s3_bucket", "local_file.2.gz")

    reset_mocks()
    r.run_copy(
        "/path/local_file",
        "s3_bucket",
        "table_name",
        delim=",",
        copy_options=["SOME OPTION"],
        compress=False,
    )
    # assert
    assert not mock_split_file.called
    assert not mock_compress_file.called
    assert not mock_remove.called
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
    r.run_copy(
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
    assert not mock_compress_file.called
    assert not mock_remove.called
    mock_s3_upload.assert_has_calls(expected_calls_no_folder)
    mock_rs_copy.assert_called_with(
        "table_name", "s3://s3_bucket/local_file", "|", copy_options=["SOME OPTION"]
    )
    assert not mock_s3_delete.called

    # with a s3_folder included and no splits
    reset_mocks()
    mock_split_file.return_value = [
        "/path/local_file.0",
        "/path/local_file.1",
        "/path/local_file.2",
    ]
    r.run_copy(
        "/path/local_file.txt",
        "s3_bucket",
        "table_name",
        delim="|",
        copy_options=["SOME OPTION"],
        compress=False,
        s3_folder="test",
    )
    # assert
    assert not mock_split_file.called
    assert not mock_compress_file.called
    assert not mock_remove.called
    mock_s3_upload.assert_called_with("/path/local_file.txt", "s3_bucket", "test/local_file.txt")
    mock_rs_copy.assert_called_with(
        "table_name", "s3://s3_bucket/test/local_file", "|", copy_options=["SOME OPTION"]
    )
    assert not mock_s3_delete.called

    # with a s3_folder included and splits
    reset_mocks()
    r.run_copy(
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
    assert not mock_compress_file.called
    assert not mock_remove.called
    mock_s3_upload.assert_has_calls(expected_calls_folder)
    mock_rs_copy.assert_called_with(
        "table_name", "s3://s3_bucket/test/local_file", "|", copy_options=["SOME OPTION"]
    )
    assert mock_s3_delete.called_with("s3_bucket", "test/local_file.0")
    assert mock_s3_delete.called_with("s3_bucket", "test/local_file.1")
    assert mock_s3_delete.called_with("s3_bucket", "test/local_file.2")

    # with a s3_folder included , splits, and gzip
    reset_mocks()
    r.run_copy(
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
    assert mock_compress_file.called
    assert mock_remove.called
    mock_s3_upload.assert_has_calls(expected_calls_folder_gzip)
    mock_rs_copy.assert_called_with(
        "table_name", "s3://s3_bucket/test/local_file", "|", copy_options=["SOME OPTION", "GZIP"]
    )
    assert not mock_s3_delete.called


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_redshift_copy_to_redshift(mock_session, rs_creds, dbapi):

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test_redshift = locopy.S3(dbapi=dbapi, **rs_creds)
    test_redshift._copy_to_redshift("table", "s3bucket")

    assert mock_connect.return_value.cursor.return_value.execute.called

    (
        mock_connect.return_value.cursor.return_value.execute.assert_called_with(
            "COPY table FROM 's3bucket' CREDENTIALS "
            "'aws_access_key_id={0};aws_secret_access_key={1};token={2}' "
            "DELIMITER '|' DATEFORMAT 'auto' COMPUPDATE ON "
            "TRUNCATECOLUMNS;".format(
                test_redshift.session.get_credentials().access_key,
                test_redshift.session.get_credentials().secret_key,
                test_redshift.session.get_credentials().token,
            ),
            None,
        )
    )

    # tab delim
    test_redshift._copy_to_redshift("table", "s3bucket", delim="\t")

    assert mock_connect.return_value.cursor.return_value.execute.called

    (
        mock_connect.return_value.cursor.return_value.execute.assert_called_with(
            "COPY table FROM 's3bucket' CREDENTIALS "
            "'aws_access_key_id={0};aws_secret_access_key={1};token={2}' "
            "DELIMITER '\t' DATEFORMAT 'auto' COMPUPDATE ON "
            "TRUNCATECOLUMNS;".format(
                test_redshift.session.get_credentials().access_key,
                test_redshift.session.get_credentials().secret_key,
                test_redshift.session.get_credentials().token,
            ),
            None,
        )
    )


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
@mock.patch("locopy.locopy.Cmd._is_connected")
def test_redshift_copy_to_redshift_exception(mock_connected, mock_session, rs_creds, dbapi):

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        test_redshift = locopy.S3(dbapi=dbapi, **rs_creds)

    mock_connected.return_value = False
    with pytest.raises(RedshiftConnectionError):
        test_redshift._copy_to_redshift("table", "s3bucket")

    mock_connected.return_value = True
    (mock_connect.return_value.cursor.return_value.execute.side_effect) = Exception(
        "COPY Exception"
    )
    with pytest.raises(RedshiftError):
        test_redshift._copy_to_redshift("table", "s3bucket")


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.os.remove")
@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.write_file")
@mock.patch("locopy.s3.S3._get_column_names")
@mock.patch("locopy.s3.S3._unload_generated_files")
@mock.patch("locopy.s3.S3._unload_to_s3")
@mock.patch("locopy.s3.S3._generate_unload_path")
@mock.patch("locopy.s3.Session")
def test_unload(
    mock_session,
    mock_generate_unload_path,
    mock_unload_to_s3,
    mock_unload_generated_files,
    mock_get_col_names,
    mock_write,
    mock_delete_from_s3,
    mock_remove,
    rs_creds,
    dbapi,
):
    def reset_mocks():
        mock_session.reset_mock()
        mock_generate_unload_path.reset_mock()
        mock_unload_generated_files.reset_mock()
        mock_get_col_names.reset_mock()
        mock_write.reset_mock()
        mock_delete_from_s3.reset_mock()
        mock_remove.reset_mock()

    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)

    ## Test 1: check that basic export pipeline functions are called
    mock_unload_generated_files.return_value = ["dummy_file"]
    mock_get_col_names.return_value = ["dummy_col_name"]
    mock_generate_unload_path.return_value = "dummy_s3_path"

    ## ensure nothing is returned when read=False
    assert (
        r.run_unload(
            query="query",
            s3_bucket="s3_bucket",
            s3_folder=None,
            export_path=False,
            delimiter=",",
            delete_s3_after=False,
            parallel_off=False,
        )
        is None
    )

    assert mock_unload_generated_files.called

    assert not mock_write.called, "write_file should only be called " "if export_path != False"
    mock_generate_unload_path.assert_called_with("s3_bucket", None)
    mock_get_col_names.assert_called_with("query")
    mock_unload_to_s3.assert_called_with(
        query="query", s3path="dummy_s3_path", unload_options=["DELIMITER ','"]
    )

    ## Test 2: different delimiter
    reset_mocks()
    mock_unload_generated_files.return_value = ["dummy_file"]
    mock_get_col_names.return_value = ["dummy_col_name"]
    mock_generate_unload_path.return_value = "dummy_s3_path"
    assert (
        r.run_unload(
            query="query",
            s3_bucket="s3_bucket",
            s3_folder=None,
            export_path=False,
            delimiter="|",
            delete_s3_after=False,
            parallel_off=True,
        )
        is None
    )

    ## check that unload options are modified based on supplied args
    mock_unload_to_s3.assert_called_with(
        query="query", s3path="dummy_s3_path", unload_options=["DELIMITER '|'", "PARALLEL OFF"]
    )

    ## Test 3: ensure exception is raised when no column names are retrieved
    reset_mocks()
    mock_unload_generated_files.return_value = ["dummy_file"]
    mock_generate_unload_path.return_value = "dummy_s3_path"
    mock_get_col_names.return_value = None
    with pytest.raises(Exception):
        r.run_unload("query", "s3_bucket", None)

    ## Test 4: ensure exception is raised when no files are returned
    reset_mocks()
    mock_generate_unload_path.return_value = "dummy_s3_path"
    mock_get_col_names.return_value = ["dummy_col_name"]
    mock_unload_generated_files.return_value = None
    with pytest.raises(Exception):
        r.run_unload("query", "s3_bucket", None)

    ## Test 5: ensure file writing is initiated when export_path is supplied
    reset_mocks()
    mock_get_col_names.return_value = ["dummy_col_name"]
    mock_generate_unload_path.return_value = "dummy_s3_path"
    mock_unload_generated_files.return_value = ["/dummy_file"]
    with mock.patch("locopy.s3.open") as mock_open:
        r.run_unload(
            query="query",
            s3_bucket="s3_bucket",
            s3_folder=None,
            export_path="my_output.csv",
            delimiter=",",
            delete_s3_after=True,
            parallel_off=False,
        )
        mock_open.assert_called_with("my_output.csv", "ab")
    assert mock_write.called
    assert mock_delete_from_s3.called_with("s3_bucket", "my_output.csv")


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_unload_generated_fields(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)
    r._unload_generated_files()

    assert mock_connect.return_value.cursor.return_value.execute.called
    assert mock_connect.return_value.cursor.return_value.fetchall.called


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_get_column_names(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)
    r._get_column_names("query")
    sql = "SELECT * FROM (query) WHERE 1 = 0"

    assert mock_connect.return_value.cursor.return_value.execute.called_with(sql, None)


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_unload_to_s3(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + ".connect") as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)
    r._unload_to_s3("query", "path")

    assert mock_connect.return_value.cursor.return_value.execute.called
