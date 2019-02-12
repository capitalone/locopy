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
import tempfile
import pytest
import pg8000
import psycopg2
import locopy

from unittest import mock
from locopy.errors import (
    S3Error,
    S3CredentialsError,
    S3InitializationError,
    S3UploadError,
    S3DownloadError,
    S3DeletionError,
)

PROFILE = "test"
BAD_PROFILE = "not real"
S3_DEFAULT_BUCKET = "test bucket"
LOCAL_TEST_FILE = "test file"
CUSTOM_KEY = "custom key"
KMS_KEY = "arn:aws:kms:us-east-1:9999999:key/0x0x0x0x0"
GOOD_CONFIG_YAML = """host: host
port: 1234
dbname: db
user: userid
password: pass"""

DBAPIS = [pg8000, psycopg2]


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_session_profile_without_kms(mock_session, dbapi):
    s = locopy.S3(profile=PROFILE)
    mock_session.assert_called_with(profile_name=PROFILE)
    assert s.kms_key == None


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_session_profile_with_kms(mock_session, dbapi):
    s = locopy.S3(profile=PROFILE, kms_key=KMS_KEY)
    mock_session.assert_called_with(profile_name=PROFILE)
    assert s.kms_key == KMS_KEY


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_session_profile_without_any(mock_session, dbapi):
    s = locopy.S3()
    mock_session.assert_called_with(profile_name=None)
    assert s.kms_key == None


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Session")
def test_mock_s3_init_exception(mock_session, dbapi):
    mock_session.side_effect = S3Error()
    with pytest.raises(S3Error):
        locopy.S3()

    mock_session.side_effect = None
    mock_session.return_value.get_credentials.return_value = None
    with pytest.raises(S3CredentialsError):
        locopy.S3()


@pytest.mark.parametrize("dbapi", DBAPIS)
@mock.patch("locopy.s3.Config")
@mock.patch("locopy.s3.Session")
def test_mock_s3_set_client(mock_session, mock_config, dbapi):
    s = locopy.S3(profile=PROFILE)
    mock_config.assert_called_with(signature_version="s3v4")

    mock_config.side_effect = Exception("_set_client Exception")
    with pytest.raises(S3InitializationError):
        locopy.S3(profile=PROFILE)


@mock.patch("locopy.s3.Session.get_credentials")
def test_get_credentials(mock_cred, aws_creds):
    s = locopy.S3()
    mock_cred.return_value = aws_creds
    cred_string = s._credentials_string()
    expected = "aws_access_key_id=access;" "aws_secret_access_key=secret;" "token=token"
    assert cred_string == expected

    aws_creds.token = None
    mock_cred.return_value = aws_creds
    cred_string = s._credentials_string()
    expected = "aws_access_key_id=access;" "aws_secret_access_key=secret"
    assert cred_string == expected

    mock_cred.side_effect = Exception("Exception")
    with pytest.raises(Exception):
        locopy.S3()


@mock.patch("locopy.s3.Session")
def test_generate_s3_path(mock_session):
    s = locopy.S3()
    assert s._generate_s3_path("TEST", "KEY") == "s3://TEST/KEY"
    assert s._generate_s3_path("TEST SPACE", "KEY SPACE") == "s3://TEST SPACE/KEY SPACE"
    assert s._generate_s3_path(None, None) == "s3://None/None"


@mock.patch("locopy.s3.Session")
def test_generate_unload_path(mock_session):
    s = locopy.S3()
    assert s._generate_unload_path("TEST", "FOLDER/") == "s3://TEST/FOLDER/"
    assert s._generate_unload_path("TEST SPACE", "FOLDER SPACE/") == "s3://TEST SPACE/FOLDER SPACE/"
    assert s._generate_unload_path("TEST", "PREFIX") == "s3://TEST/PREFIX"
    assert s._generate_unload_path("TEST", None) == "s3://TEST"


@mock.patch("locopy.s3.ProgressPercentage")
@mock.patch("locopy.s3.Session")
def test_upload_to_s3(mock_session, mock_progress):
    mock_progress.return_value = None
    s = locopy.S3()
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, LOCAL_TEST_FILE)
    mock_progress.assert_called_with("test file")
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        os.path.basename(LOCAL_TEST_FILE),
        ExtraArgs={"ServerSideEncryption": "AES256"},
        Callback=None,
    )

    s = locopy.S3(kms_key=KMS_KEY)
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, LOCAL_TEST_FILE)
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        os.path.basename(LOCAL_TEST_FILE),
        ExtraArgs={"SSEKMSKeyId": KMS_KEY, "ServerSideEncryption": "aws:kms"},
        Callback=None,
    )


@mock.patch("locopy.s3.ProgressPercentage")
@mock.patch("locopy.s3.Session")
def test_upload_to_s3_with_custom_key(mock_session, mock_progress):
    mock_progress.return_value = None
    s = locopy.S3()
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, CUSTOM_KEY)
    mock_progress.assert_called_with("test file")
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        CUSTOM_KEY,
        ExtraArgs={"ServerSideEncryption": "AES256"},
        Callback=None,
    )

    s = locopy.S3(kms_key=KMS_KEY)
    s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, CUSTOM_KEY)
    s.s3.upload_file.assert_called_with(
        LOCAL_TEST_FILE,
        S3_DEFAULT_BUCKET,
        CUSTOM_KEY,
        ExtraArgs={"SSEKMSKeyId": KMS_KEY, "ServerSideEncryption": "aws:kms"},
        Callback=None,
    )


@mock.patch("locopy.s3.ProgressPercentage")
@mock.patch("locopy.s3.Session")
def test_upload_to_s3_exception(mock_session, mock_progress):
    mock_progress.return_value = None
    s = locopy.S3()
    s.s3.upload_file.side_effect = Exception("Upload Exception")
    with pytest.raises(S3UploadError):
        s.upload_to_s3(LOCAL_TEST_FILE, S3_DEFAULT_BUCKET, CUSTOM_KEY)


@mock.patch("locopy.s3.S3.upload_to_s3")
@mock.patch("locopy.s3.Session")
def test_upload_list_to_s3_single_with_folder(mock_session, mock_upload):
    calls = [mock.call("test.1", "test_bucket", "test_folder/test.1")]
    s = locopy.S3()
    res = s.upload_list_to_s3(["test.1"], "test_bucket", "test_folder")
    assert res == ["test_bucket/test_folder/test.1"]
    mock_upload.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.upload_to_s3")
@mock.patch("locopy.s3.Session")
def test_upload_list_to_s3_single_without_folder(mock_session, mock_upload):
    calls = [mock.call("test.1", "test_bucket", "test.1")]
    s = locopy.S3()
    res = s.upload_list_to_s3(["test.1"], "test_bucket")
    assert res == ["test_bucket/test.1"]
    mock_upload.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.upload_to_s3")
@mock.patch("locopy.s3.Session")
def test_upload_list_to_s3_multiple_with_folder(mock_session, mock_upload):
    calls = [
        mock.call("test.1", "test_bucket", "test_folder/test.1"),
        mock.call("test.2", "test_bucket", "test_folder/test.2"),
    ]
    s = locopy.S3()
    res = s.upload_list_to_s3(["test.1", "test.2"], "test_bucket", "test_folder")
    assert res == ["test_bucket/test_folder/test.1", "test_bucket/test_folder/test.2"]
    mock_upload.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.upload_to_s3")
@mock.patch("locopy.s3.Session")
def test_upload_list_to_s3_multiple_without_folder(mock_session, mock_upload):
    calls = [
        mock.call("test.1", "test_bucket", "test.1"),
        mock.call("test.2", "test_bucket", "test.2"),
    ]
    s = locopy.S3()
    res = s.upload_list_to_s3(["test.1", "test.2"], "test_bucket")
    assert res == ["test_bucket/test.1", "test_bucket/test.2"]
    mock_upload.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.upload_to_s3")
@mock.patch("locopy.s3.Session")
def test_upload_list_to_s3_exception(mock_session, mock_upload):
    s = locopy.S3()
    mock_upload.side_effect = S3UploadError("Upload Exception")
    with pytest.raises(S3UploadError):
        s.upload_list_to_s3(["test1", "test2"], "test_bucket", "test_folder")


@mock.patch("locopy.s3.TransferConfig")
@mock.patch("locopy.s3.Session")
def test_download_from_s3(mock_session, mock_config):
    s = locopy.S3()
    s.download_from_s3(S3_DEFAULT_BUCKET, LOCAL_TEST_FILE, LOCAL_TEST_FILE)
    s.s3.download_file.assert_called_with(
        S3_DEFAULT_BUCKET, LOCAL_TEST_FILE, os.path.basename(LOCAL_TEST_FILE), Config=mock_config()
    )

    mock_config.side_effect = Exception()
    with pytest.raises(S3DownloadError):
        s.download_from_s3(S3_DEFAULT_BUCKET, LOCAL_TEST_FILE, LOCAL_TEST_FILE)


@mock.patch("locopy.s3.Session")
def test_delete_from_s3(mock_session):
    s = locopy.S3()
    s.delete_from_s3("TEST_BUCKET", "TEST_FILE")
    s.s3.delete_object.assert_called_with(Bucket="TEST_BUCKET", Key="TEST_FILE")


@mock.patch("locopy.s3.Session")
def test_delete_from_s3_exception(mock_session):
    s = locopy.S3()
    s.s3.delete_object.side_effect = Exception("Delete Exception")
    with pytest.raises(S3DeletionError):
        s.delete_from_s3("TEST_BUCKET", "TEST_FILE")


@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.Session")
def test_delete_list_from_s3_single_with_folder(mock_session, mock_delete):
    calls = [mock.call("test_bucket", "test_folder/test.1")]
    s = locopy.S3()
    s.delete_list_from_s3(["test_bucket/test_folder/test.1"])
    mock_delete.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.Session")
def test_delete_list_from_s3_single_without_folder(mock_session, mock_delete):
    calls = [mock.call("test_bucket", "test.1")]
    s = locopy.S3()
    s.delete_list_from_s3(["test_bucket/test.1"])
    mock_delete.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.Session")
def test_delete_list_from_s3_multiple_with_folder(mock_session, mock_delete):
    calls = [
        mock.call("test_bucket", "test_folder/test.1"),
        mock.call("test_bucket", "test_folder/test.2"),
    ]
    s = locopy.S3()
    s.delete_list_from_s3(["test_bucket/test_folder/test.1", "test_bucket/test_folder/test.2"])
    mock_delete.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.Session")
def test_delete_list_from_s3_multiple_without_folder(mock_session, mock_delete):
    calls = [mock.call("test_bucket", "test.1"), mock.call("test_bucket", "test.2")]
    s = locopy.S3()
    s.delete_list_from_s3(["test_bucket/test.1", "test_bucket/test.2"])
    mock_delete.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.Session")
def test_delete_list_from_s3_single_with_folder_and_special_chars(mock_session, mock_delete):
    calls = [mock.call("test_bucket", r"test_folder/#$#@$@#$dffksdojfsdf\\\\\/test.1")]
    s = locopy.S3()
    s.delete_list_from_s3([r"test_bucket/test_folder/#$#@$@#$dffksdojfsdf\\\\\/test.1"])
    mock_delete.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.delete_from_s3")
@mock.patch("locopy.s3.Session")
def test_delete_list_from_s3_exception(mock_session, mock_delete):
    s = locopy.S3()
    mock_delete.side_effect = S3UploadError("Upload Exception")
    with pytest.raises(S3UploadError):
        s.delete_list_from_s3(["test_bucket/test_folder/test.1", "test_bucket/test_folder/test.2"])


@mock.patch("locopy.s3.Session")
def test_parse_s3_url(mock_session):
    s = locopy.S3()
    assert s.parse_s3_url("s3://bucket/folder/file.txt") == ("bucket", "folder/file.txt")
    assert s.parse_s3_url("s3://bucket/folder/") == ("bucket", "folder/")
    assert s.parse_s3_url("s3://bucket") == ("bucket", "")
    assert s.parse_s3_url(r"s3://bucket/!@#$%\\\/file.txt") == ("bucket", r"!@#$%\\\/file.txt")
    assert s.parse_s3_url("s3://") == ("", "")

    assert s.parse_s3_url("bucket/folder/file.txt") == ("bucket", "folder/file.txt")
    assert s.parse_s3_url("bucket/folder/") == ("bucket", "folder/")
    assert s.parse_s3_url("bucket") == ("bucket", "")
    assert s.parse_s3_url(r"bucket/!@#$%\\\/file.txt") == ("bucket", r"!@#$%\\\/file.txt")
    assert s.parse_s3_url("") == ("", "")


@mock.patch("locopy.s3.S3.download_from_s3")
@mock.patch("locopy.s3.Session")
def test_download_list_from_s3_single(mock_session, mock_download):
    calls = [mock.call("bucket", "test.1", os.path.join(os.getcwd(), "test.1"))]
    s = locopy.S3()
    res = s.download_list_from_s3(["s3://bucket/test.1"])
    assert res == [os.path.join(os.getcwd(), "test.1")]
    mock_download.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.download_from_s3")
@mock.patch("locopy.s3.Session")
def test_download_list_from_s3_single_with_localpath(mock_session, mock_download):
    tmp_path = tempfile.TemporaryDirectory()
    calls = [mock.call("bucket", "test.1", os.path.join(tmp_path.name, "test.1"))]
    s = locopy.S3()
    res = s.download_list_from_s3(["s3://bucket/test.1"], tmp_path.name)
    assert res == [os.path.join(tmp_path.name, "test.1")]
    mock_download.assert_has_calls(calls)
    tmp_path.cleanup()


@mock.patch("locopy.s3.S3.download_from_s3")
@mock.patch("locopy.s3.Session")
def test_download_list_from_s3_multiple(mock_session, mock_download):
    calls = [
        mock.call("bucket", "test.1", os.path.join(os.getcwd(), "test.1")),
        mock.call("bucket", "test.2", os.path.join(os.getcwd(), "test.2")),
    ]
    s = locopy.S3()
    res = s.download_list_from_s3(["s3://bucket/test.1", "s3://bucket/test.2"])
    assert res == [os.path.join(os.getcwd(), "test.1"), os.path.join(os.getcwd(), "test.2")]
    mock_download.assert_has_calls(calls)


@mock.patch("locopy.s3.S3.download_from_s3")
@mock.patch("locopy.s3.Session")
def test_download_list_from_s3_multiple_with_localpath(mock_session, mock_download):
    tmp_path = tempfile.TemporaryDirectory()
    calls = [
        mock.call("bucket", "test.1", os.path.join(tmp_path.name, "test.1")),
        mock.call("bucket", "test.2", os.path.join(tmp_path.name, "test.2")),
    ]
    s = locopy.S3()
    res = s.download_list_from_s3(["s3://bucket/test.1", "s3://bucket/test.2"], tmp_path.name)
    assert res == [os.path.join(tmp_path.name, "test.1"), os.path.join(tmp_path.name, "test.2")]
    mock_download.assert_has_calls(calls)
    tmp_path.cleanup()


@mock.patch("locopy.s3.S3.download_from_s3")
@mock.patch("locopy.s3.Session")
def test_download_list_from_s3_exception(mock_session, mock_download):
    s = locopy.S3()
    mock_download.side_effect = S3DownloadError("Download Exception")
    with pytest.raises(S3DownloadError):
        s.download_list_from_s3(["s3://bucket/test.1", "s3://bucket/test.2"])
