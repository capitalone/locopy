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
import pytest
import pg8000, psycopg2
import locopy

from unittest import mock
from datetime import date
from botocore.credentials import Credentials
from locopy.errors import (S3Error, S3CredentialsError, S3InitializationError,
                           RedshiftCredentialsError, S3UploadError,
                           S3DeletionError, RedshiftConnectionError,
                           RedshiftError)
from tests import MockS3



PROFILE = 'test'
BAD_PROFILE = 'not real'
S3_DEFAULT_BUCKET = 'test bucket'
LOCAL_TEST_FILE = 'test file'
CUSTOM_KEY = 'custom key'
KMS_KEY = 'arn:aws:kms:us-east-1:9999999:key/0x0x0x0x0'
GOOD_CONFIG_YAML = """host: host
port: 1234
dbname: db
user: userid
password: pass"""


DBAPIS = [pg8000, psycopg2]


def test_add_default_copy_options():
    assert locopy.s3.add_default_copy_options() == ["DATEFORMAT 'auto'",
                                                    "COMPUPDATE ON",
                                                    "TRUNCATECOLUMNS"]
    assert locopy.s3.add_default_copy_options(
        ["DATEFORMAT 'other'", "NULL AS 'blah'"]) == ["DATEFORMAT 'other'",
                                                      "NULL AS 'blah'",
                                                      "COMPUPDATE ON",
                                                      "TRUNCATECOLUMNS"]


def test_combine_copy_options():
    assert locopy.s3.combine_copy_options(
        locopy.s3.add_default_copy_options()) == ("DATEFORMAT 'auto' COMPUPDATE "
                                                  "ON TRUNCATECOLUMNS")


@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_session_without_kms(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        s = locopy.S3(dbapi=dbapi, **rs_creds)
    mock_session.assert_called_with(profile_name=None)
    assert s.kms_key == None



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_session_with_profile(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        s = locopy.S3(dbapi=dbapi, profile=PROFILE, **rs_creds)
    mock_session.assert_called_with(profile_name=PROFILE)
    assert s.kms_key == None



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_session_profile_with_kms(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        s = locopy.S3(dbapi=dbapi, kms_key=KMS_KEY, **rs_creds)
    mock_session.assert_called_with(profile_name=None)
    assert s.kms_key == KMS_KEY



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_init_exception(mock_session, dbapi):
    with pytest.raises(RedshiftCredentialsError):
        locopy.S3(dbapi=dbapi, host='host', port='port', dbname='dbname',
                user='user')
    with pytest.raises(RedshiftCredentialsError):
        locopy.S3(dbapi=dbapi, host='host', port='port', dbname='dbname',
                password='password')
    with pytest.raises(RedshiftCredentialsError):
        locopy.S3(dbapi=dbapi, host='host', port='port', user='user',
                password='password')
    with pytest.raises(RedshiftCredentialsError):
        locopy.S3(dbapi=dbapi, host='host', dbname='dbname', user='user',
                password='password')
    with pytest.raises(RedshiftCredentialsError):
        locopy.S3(dbapi=dbapi, port='port', dbname='dbname', user='user',
                password='password')
    with pytest.raises(RedshiftCredentialsError):
        locopy.S3(dbapi=dbapi, port='port', dbname='dbname', user='user',
                password='password', kms_key=KMS_KEY)




@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_set_session_exception(mock_session, dbapi):
    mock_session.side_effect = Exception("_set_session exception")
    with pytest.raises(S3Error):
        with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
            locopy.S3(dbapi=dbapi, host='host', port='port', dbname='dbname',
                    user='user', password='password')



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch("locopy.s3.Config")
@mock.patch("locopy.s3.Session")
def test_mock_s3_set_client(mock_session, mock_config, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        s = locopy.S3(dbapi=dbapi, **rs_creds)
    mock_config.assert_called_with(signature_version='s3v4')

    mock_config.side_effect = Exception("_set_client Exception")
    with pytest.raises(S3InitializationError):
        with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
            locopy.S3(dbapi=dbapi, **rs_creds)




@mock.patch('locopy.s3.ProgressPercentage')
@mock.patch('locopy.s3.Session')
def test_mock_s3_upload_default_key(mock_session, mock_progress):
    mock_progress.return_value = None
    s = MockS3()
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, LOCAL_TEST_FILE)
    mock_progress.assert_called_with('test file')
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        os.path.basename(LOCAL_TEST_FILE),
        ExtraArgs={'ServerSideEncryption': 'AES256'},
        Callback=None)

    s = MockS3(kms_key=KMS_KEY)
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, LOCAL_TEST_FILE)
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        os.path.basename(LOCAL_TEST_FILE),
        ExtraArgs={'SSEKMSKeyId': KMS_KEY, 'ServerSideEncryption': 'aws:kms'},
        Callback=None)




@mock.patch('locopy.s3.ProgressPercentage')
@mock.patch('locopy.s3.Session')
def test_mock_s3_upload_with_key(mock_session, mock_progress, rs_creds):
    mock_progress.return_value = None

    s = MockS3()
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, CUSTOM_KEY)
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        CUSTOM_KEY,
        ExtraArgs={'ServerSideEncryption': 'AES256'},
        Callback=None)

    s = MockS3(kms_key=KMS_KEY, **rs_creds)
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, CUSTOM_KEY)
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        CUSTOM_KEY,
        ExtraArgs={'SSEKMSKeyId': KMS_KEY, 'ServerSideEncryption': 'aws:kms'},
        Callback=None)




@mock.patch('locopy.s3.ProgressPercentage')
@mock.patch("locopy.s3.Session")
def test_mock_s3_upload_exception(mock_session, mock_progress):
    mock_progress.return_value = None
    s = MockS3()
    s.s3.upload_file.side_effect = Exception("Upload Exception")
    with pytest.raises(S3UploadError):
        s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, CUSTOM_KEY)




@mock.patch('locopy.s3.Session')
def test_generate_s3_path(mock_session):
    s = MockS3()
    assert s._generate_s3_path("TEST", "KEY") == "s3://TEST/KEY"
    assert s._generate_s3_path("TEST SPACE", "KEY SPACE") == "s3://TEST SPACE/KEY SPACE"
    assert s._generate_s3_path(None, None) == "s3://None/None"




@mock.patch('locopy.s3.Session')
def test_generate_unload_path(mock_session):
    s = MockS3()
    assert s._generate_unload_path("TEST", "FOLDER/") == "s3://TEST/FOLDER/"
    assert s._generate_unload_path("TEST SPACE", "FOLDER SPACE/") == "s3://TEST SPACE/FOLDER SPACE/"
    assert s._generate_unload_path("TEST", "PREFIX") == "s3://TEST/PREFIX"
    assert s._generate_unload_path("TEST", None) == "s3://TEST"



@mock.patch('locopy.s3.Session')
def test_mock_s3_delete(mock_session):
    s = MockS3()
    s.delete_from_s3("TEST_BUCKET", "TEST_FILE")
    s.s3.delete_object.assert_called_with(Bucket="TEST_BUCKET", Key="TEST_FILE")





@mock.patch('locopy.s3.Session')
def test_mock_s3_delete_exception(mock_session):
    s = MockS3()
    s.s3.delete_object.side_effect = Exception("Delete Exception")
    with pytest.raises(S3DeletionError):
        s.delete_from_s3("TEST_BUCKET", "TEST_FILE")




@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.Session')
def test_super_init(mock_session, rs_creds, dbapi):
    # Test that the super class (locopy) gets the right stuff
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        s = locopy.S3(dbapi=dbapi, **rs_creds)
    assert s.host == "host"
    assert s.port == "port"
    assert s.dbname == "dbname"
    assert s.user == "user"
    assert s.password == "password"

    if dbapi.__name__ == 'pg8000':
        mock_connect.assert_called_with(
            host='host', user='user', port='port', password='password',
            database='dbname', ssl=True)
    else:
        mock_connect.assert_called_with(
            host='host', user='user', port='port', password='password',
            database='dbname', sslmode='require')




@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.utility.open', mock.mock_open(read_data=GOOD_CONFIG_YAML))
@mock.patch('locopy.s3.Session')
def test_super_init_yaml(mock_session, dbapi):
    # Test that the super class (locopy) gets the right stuff with a yaml
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        s = locopy.S3(dbapi=dbapi, config_yaml='MY_YAML_FILE.yml')
    assert s.host == "host"
    assert s.port == 1234
    assert s.dbname == 'db'
    assert s.user == "userid"
    assert s.password == "pass"

    if dbapi.__name__ == 'pg8000':
        mock_connect.assert_called_with(
            host='host', user='userid', port=1234, password='pass', database='db',
            ssl=True)
    else:
        mock_connect.assert_called_with(
            host='host', user='userid', port=1234, password='pass', database='db',
            sslmode='require')




@mock.patch('locopy.s3.Session.get_credentials')
def test_get_profile_tokens(mock_cred, aws_creds):
    r = MockS3()
    mock_cred.return_value = aws_creds
    cred_string = r._credentials_string()
    expected = ('aws_access_key_id=access;'
                'aws_secret_access_key=secret;'
                'token=token')
    assert cred_string == expected




@mock.patch('locopy.s3.Session.get_credentials')
def test_get_profile_tokens_no_token(mock_cred):
    r = MockS3()
    mock_cred.return_value = Credentials(
        "aws_access_key_id", "aws_secret_access_key")
    cred_string = r._credentials_string()
    expected = ('aws_access_key_id=aws_access_key_id;'
                'aws_secret_access_key=aws_secret_access_key')
    assert cred_string == expected




@mock.patch('locopy.s3.Session.get_credentials')
def test_get_profile_tokens_exception(mock_cred):
    mock_cred.side_effect = Exception('Exception')
    with pytest.raises(Exception):
        MockS3()




@mock.patch('locopy.s3.Session.get_credentials')
def test_get_credentials_exception(mock_cred):
    mock_cred.reset_mock()
    mock_cred.return_value = None
    with pytest.raises(S3CredentialsError):
        MockS3()



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.os.remove')
@mock.patch('locopy.s3.S3._copy_to_redshift')
@mock.patch('locopy.s3.S3.upload_to_s3')
@mock.patch('locopy.s3.S3.delete_from_s3')
@mock.patch('locopy.s3.Session')
@mock.patch('locopy.s3.compress_file')
@mock.patch('locopy.s3.split_file')
def test_run_copy(
    mock_split_file, mock_compress_file, mock_session, mock_s3_delete,
    mock_s3_upload, mock_rs_copy, mock_remove, rs_creds, dbapi):

    def reset_mocks():
        mock_split_file.reset_mock()
        mock_compress_file.reset_mock()
        mock_s3_upload.reset_mock()
        mock_s3_delete.reset_mock()
        mock_rs_copy.reset_mock()
        mock_remove.reset_mock()

    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)

    expected_calls_no_folder = [
        mock.call('/path/local_file.0', 's3_bucket', 'local_file.0'),
        mock.call('/path/local_file.1', 's3_bucket', 'local_file.1'),
        mock.call('/path/local_file.2', 's3_bucket', 'local_file.2')]

    expected_calls_no_folder_gzip = [
        mock.call('/path/local_file.0.gz', 's3_bucket', 'local_file.0.gz'),
        mock.call('/path/local_file.1.gz', 's3_bucket', 'local_file.1.gz'),
        mock.call('/path/local_file.2.gz', 's3_bucket', 'local_file.2.gz')]

    expected_calls_folder = [
        mock.call('/path/local_file.0', 's3_bucket', 'test/local_file.0'),
        mock.call('/path/local_file.1', 's3_bucket', 'test/local_file.1'),
        mock.call('/path/local_file.2', 's3_bucket', 'test/local_file.2')]

    expected_calls_folder_gzip = [
        mock.call('/path/local_file.0.gz', 's3_bucket', 'test/local_file.0.gz'),
        mock.call('/path/local_file.1.gz', 's3_bucket', 'test/local_file.1.gz'),
        mock.call( '/path/local_file.2.gz', 's3_bucket', 'test/local_file.2.gz')]

    r.run_copy(
        '/path/local_file.txt', 's3_bucket', 'table_name', delim="|",
        copy_options=['SOME OPTION'])

    # assert
    assert not mock_split_file.called
    mock_compress_file.assert_called_with(
        '/path/local_file.txt', '/path/local_file.txt.gz')
    mock_remove.assert_called_with('/path/local_file.txt')
    mock_s3_upload.assert_called_with(
        '/path/local_file.txt.gz', 's3_bucket', 'local_file.txt.gz')
    mock_rs_copy.assert_called_with(
        'table_name',
        's3://s3_bucket/local_file', '|',
        copy_options=['SOME OPTION', 'GZIP'])
    assert not mock_s3_delete.called, 'Only delete when explicit'



    reset_mocks()
    mock_split_file.return_value = ['/path/local_file.0',
                                    '/path/local_file.1',
                                    '/path/local_file.2']
    r.run_copy(
        '/path/local_file', 's3_bucket', 'table_name', delim="|",
        copy_options=['SOME OPTION'], splits=3, delete_s3_after=True)

    # assert
    mock_split_file.assert_called_with(
        '/path/local_file', '/path/local_file', splits=3)
    mock_compress_file.assert_called_with(
        '/path/local_file.2', '/path/local_file.2.gz')
    mock_remove.assert_called_with('/path/local_file.2')
    mock_s3_upload.assert_has_calls(expected_calls_no_folder_gzip)
    mock_rs_copy.assert_called_with(
        'table_name',
        's3://s3_bucket/local_file', '|',
        copy_options=['SOME OPTION', 'GZIP'])
    assert mock_s3_delete.called_with('s3_bucket', 'local_file.0.gz')
    assert mock_s3_delete.called_with('s3_bucket', 'local_file.1.gz')
    assert mock_s3_delete.called_with('s3_bucket', 'local_file.2.gz')



    reset_mocks()
    r.run_copy(
        '/path/local_file', 's3_bucket', 'table_name', delim=",",
        copy_options=['SOME OPTION'], compress=False)
    # assert
    assert not mock_split_file.called
    assert not mock_compress_file.called
    assert not mock_remove.called
    mock_s3_upload.assert_called_with(
        '/path/local_file', 's3_bucket', 'local_file')
    mock_rs_copy.assert_called_with(
        'table_name', 's3://s3_bucket/local_file', ',',
        copy_options=['SOME OPTION'])
    assert not mock_s3_delete.called, 'Only delete when explicit'



    reset_mocks()
    mock_split_file.return_value = ['/path/local_file.0',
                                    '/path/local_file.1',
                                    '/path/local_file.2']
    r.run_copy(
        '/path/local_file', 's3_bucket', 'table_name', delim="|",
        copy_options=['SOME OPTION'], splits=3, compress=False)
    # assert
    mock_split_file.assert_called_with(
        '/path/local_file', '/path/local_file', splits=3)
    assert not mock_compress_file.called
    assert not mock_remove.called
    mock_s3_upload.assert_has_calls(expected_calls_no_folder)
    mock_rs_copy.assert_called_with(
        'table_name', 's3://s3_bucket/local_file', '|',
        copy_options=['SOME OPTION'])
    assert not mock_s3_delete.called



    # with a s3_folder included and no splits
    reset_mocks()
    mock_split_file.return_value = ['/path/local_file.0',
                                    '/path/local_file.1',
                                    '/path/local_file.2']
    r.run_copy(
        '/path/local_file.txt', 's3_bucket', 'table_name', delim="|",
        copy_options=['SOME OPTION'], compress=False, s3_folder='test')
    # assert
    assert not mock_split_file.called
    assert not mock_compress_file.called
    assert not mock_remove.called
    mock_s3_upload.assert_called_with(
        '/path/local_file.txt', 's3_bucket', 'test/local_file.txt')
    mock_rs_copy.assert_called_with(
        'table_name', 's3://s3_bucket/test/local_file', '|',
        copy_options=['SOME OPTION'])
    assert not mock_s3_delete.called



    # with a s3_folder included and splits
    reset_mocks()
    r.run_copy(
        '/path/local_file', 's3_bucket', 'table_name', delim="|",
        copy_options=['SOME OPTION'], splits=3, compress=False,
        s3_folder='test', delete_s3_after=True)
    # assert
    mock_split_file.assert_called_with(
        '/path/local_file', '/path/local_file', splits=3)
    assert not mock_compress_file.called
    assert not mock_remove.called
    mock_s3_upload.assert_has_calls(expected_calls_folder)
    mock_rs_copy.assert_called_with(
        'table_name', 's3://s3_bucket/test/local_file', '|',
        copy_options=['SOME OPTION'])
    assert mock_s3_delete.called_with('s3_bucket', 'test/local_file.0')
    assert mock_s3_delete.called_with('s3_bucket', 'test/local_file.1')
    assert mock_s3_delete.called_with('s3_bucket', 'test/local_file.2')



    # with a s3_folder included , splits, and gzip
    reset_mocks()
    r.run_copy(
        '/path/local_file', 's3_bucket', 'table_name', delim="|",
        copy_options=['SOME OPTION'], splits=3, s3_folder='test')
    # assert
    mock_split_file.assert_called_with(
        '/path/local_file', '/path/local_file', splits=3)
    assert mock_compress_file.called
    assert mock_remove.called
    mock_s3_upload.assert_has_calls(expected_calls_folder_gzip)
    mock_rs_copy.assert_called_with(
        'table_name', 's3://s3_bucket/test/local_file', '|',
        copy_options=['SOME OPTION', 'GZIP'])
    assert not mock_s3_delete.called






@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.Session')
def test_redshift_copy_to_redshift(mock_session, rs_creds, dbapi):

    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        test_redshift = locopy.S3(dbapi=dbapi, **rs_creds)
    test_redshift._copy_to_redshift("table", "s3bucket")

    assert mock_connect.return_value.cursor.return_value.execute.called

    (mock_connect.return_value
             .cursor.return_value
             .execute.assert_called_with(
                "COPY table FROM 's3bucket' CREDENTIALS "
                "'aws_access_key_id={0};aws_secret_access_key={1};token={2}' "
                "DELIMITER '|' DATEFORMAT 'auto' COMPUPDATE ON "
                "TRUNCATECOLUMNS;".format(
                    test_redshift.session.get_credentials().access_key,
                    test_redshift.session.get_credentials().secret_key,
                    test_redshift.session.get_credentials().token), None))


    # tab delim
    test_redshift._copy_to_redshift("table", "s3bucket",  delim='\t')

    assert mock_connect.return_value.cursor.return_value.execute.called

    (mock_connect.return_value
             .cursor.return_value
             .execute.assert_called_with(
                "COPY table FROM 's3bucket' CREDENTIALS "
                "'aws_access_key_id={0};aws_secret_access_key={1};token={2}' "
                "DELIMITER '\t' DATEFORMAT 'auto' COMPUPDATE ON "
                "TRUNCATECOLUMNS;".format(
                    test_redshift.session.get_credentials().access_key,
                    test_redshift.session.get_credentials().secret_key,
                    test_redshift.session.get_credentials().token), None))




@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.Session')
@mock.patch("locopy.locopy.Cmd._is_connected")
def test_redshift_copy_to_redshift_exception(
    mock_connected, mock_session, rs_creds, dbapi):

    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        test_redshift = locopy.S3(dbapi=dbapi, **rs_creds)

    mock_connected.return_value = False
    with pytest.raises(RedshiftConnectionError):
        test_redshift._copy_to_redshift("table", "s3bucket")

    mock_connected.return_value = True
    (mock_connect.return_value
             .cursor.return_value
             .execute.side_effect) = Exception('COPY Exception')
    with pytest.raises(RedshiftError):
        test_redshift._copy_to_redshift("table", "s3bucket")




@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.os.remove')
@mock.patch('locopy.s3.S3.delete_from_s3')
@mock.patch('locopy.s3.write_file')
@mock.patch('locopy.s3.S3._get_column_names')
@mock.patch('locopy.s3.S3._unload_generated_files')
@mock.patch('locopy.s3.S3._unload_to_s3')
@mock.patch('locopy.s3.S3._generate_unload_path')
@mock.patch('locopy.s3.Session')
def test_unload(
    mock_session, mock_generate_unload_path, mock_unload_to_s3,
    mock_unload_generated_files, mock_get_col_names, mock_write,
    mock_delete_from_s3, mock_remove, rs_creds, dbapi):

    def reset_mocks():
        mock_session.reset_mock()
        mock_generate_unload_path.reset_mock()
        mock_unload_generated_files.reset_mock()
        mock_get_col_names.reset_mock()
        mock_write.reset_mock()
        mock_delete_from_s3.reset_mock()
        mock_remove.reset_mock()


    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)

    ## Test 1: check that basic export pipeline functions are called
    mock_unload_generated_files.return_value = ['dummy_file']
    mock_get_col_names.return_value = ['dummy_col_name']
    mock_generate_unload_path.return_value = "dummy_s3_path"

    ## ensure nothing is returned when read=False
    assert r.run_unload(
        query="query", s3_bucket="s3_bucket",
        s3_folder=None, export_path=False, delimiter=',',
        delete_s3_after=False, parallel_off=False) is None

    assert mock_unload_generated_files.called

    assert not mock_write.called, ('write_file should only be called '
        'if export_path != False')
    mock_generate_unload_path.assert_called_with("s3_bucket", None)
    mock_get_col_names.assert_called_with("query")
    mock_unload_to_s3.assert_called_with(query="query",
        s3path="dummy_s3_path", unload_options=["DELIMITER ','"])




    ## Test 2: different delimiter
    reset_mocks()
    mock_unload_generated_files.return_value = ['dummy_file']
    mock_get_col_names.return_value = ['dummy_col_name']
    mock_generate_unload_path.return_value = "dummy_s3_path"
    assert r.run_unload(
        query="query", s3_bucket="s3_bucket",
        s3_folder=None, export_path=False, delimiter='|',
        delete_s3_after=False, parallel_off=True) is None

    ## check that unload options are modified based on supplied args
    mock_unload_to_s3.assert_called_with(
        query="query", s3path="dummy_s3_path",
        unload_options=["DELIMITER '|'", "PARALLEL OFF"])




    ## Test 3: ensure exception is raised when no column names are retrieved
    reset_mocks()
    mock_unload_generated_files.return_value = ['dummy_file']
    mock_generate_unload_path.return_value = "dummy_s3_path"
    mock_get_col_names.return_value = None
    with pytest.raises(Exception):
        r.run_unload("query", "s3_bucket", None)




    ## Test 4: ensure exception is raised when no files are returned
    reset_mocks()
    mock_generate_unload_path.return_value = "dummy_s3_path"
    mock_get_col_names.return_value = ['dummy_col_name']
    mock_unload_generated_files.return_value = None
    with pytest.raises(Exception):
        r.run_unload("query", "s3_bucket", None)



    ## Test 5: ensure file writing is initiated when export_path is supplied
    reset_mocks()
    mock_get_col_names.return_value = ['dummy_col_name']
    mock_generate_unload_path.return_value = "dummy_s3_path"
    mock_unload_generated_files.return_value = ['/dummy_file']
    with mock.patch("locopy.s3.open") as mock_open:
        r.run_unload(query="query", s3_bucket="s3_bucket",
            s3_folder=None, export_path="my_output.csv", delimiter=',',
            delete_s3_after=True, parallel_off=False)
        mock_open.assert_called_with('my_output.csv', 'ab')
    assert mock_write.called
    assert mock_delete_from_s3.called_with('s3_bucket', 'my_output.csv')




@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.Session')
def test_unload_generated_fields(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)
    r._unload_generated_files()

    assert mock_connect.return_value.cursor.return_value.execute.called
    assert mock_connect.return_value.cursor.return_value.fetchall.called




@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.Session')
def test_get_column_names(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)
    r._get_column_names("query")
    sql = "SELECT * FROM (query) WHERE 1 = 0"

    assert (mock_connect.return_value.cursor.return_value
            .execute.called_with(sql, None))



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.s3.Session')
def test_unload_to_s3(mock_session, rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        r = locopy.S3(dbapi=dbapi, **rs_creds)
    r._unload_to_s3("query", "path")

    assert mock_connect.return_value.cursor.return_value.execute.called
