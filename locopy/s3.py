# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2018 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""S3 Module
Module to wrap the boto3 api usage and provide functionality to manage
multipart upload to S3 buckets
"""
import os

from boto3 import Session
from boto3.s3.transfer import TransferConfig
from botocore.client import Config

from .utility import ProgressPercentage
from .logger import get_logger, DEBUG, INFO, WARN, ERROR, CRITICAL
from .errors import (
    S3Error,
    S3CredentialsError,
    S3InitializationError,
    S3UploadError,
    S3DownloadError,
    S3DeletionError,
)

logger = get_logger(__name__, DEBUG)


class S3(object):
    """
    S3 wrapper class which utilizes the boto3 library to push files to an S3
    bucket.

    Parameters
    ----------
    profile : str, optional
        The name of the AWS profile to use which is typical stored in the
        ``credentials`` file.  You can also set environment variable
        ``AWS_DEFAULT_PROFILE`` which would be used instead.

    kms_key : str, optional
        The KMS key to use for encryption
        If kms_key Defaults to ``None`` then the AES256 ServerSideEncryption
        will be used.

    **kwargs
        Optional keyword arguments.

    Attributes
    ----------
    profile : str
        String representing the AWS profile for authentication

    kms_key : str
        String representing the s3 kms key

    session : boto3.Session
        Hold the AWS session credentials / info

    s3 : botocore.client.S3
        Hold the S3 client object which is used to upload/delete files to S3

    Raises
    ------
    S3Error
        Error initializing AWS Session (ex: invalid profile)

    S3CredentialsError
        Issue with AWS credentials

    S3InitializationError
        Issue initializing S3 session
    """

    def __init__(self, profile=None, kms_key=None, **kwargs):

        self.profile = profile
        self.kms_key = kms_key
        self.session = None
        self.s3 = None

        self._set_session()
        self._set_client()

    def _set_session(self):
        try:
            self.session = Session(profile_name=self.profile)
            logger.info("Initialized AWS session.")
        except Exception as e:
            logger.error("Error initializing AWS Session, err: %s", e)
            raise S3Error("Error initializing AWS Session.")
        credentials = self.session.get_credentials()
        if credentials is None:
            raise S3CredentialsError("Credentials could not be set.")

    def _set_client(self):
        try:
            self.s3 = self.session.client("s3", config=Config(signature_version="s3v4"))
            logger.info("Successfully initialized S3 client.")
        except Exception as e:
            logger.error("Error initializing S3 Client, err: %s", e)
            raise S3InitializationError("Error initializing S3 Client.")

    def _credentials_string(self):
        """Returns a credentials string for the Redshift COPY or UNLOAD command,
        containing credentials from the current session.

        Returns
        -------
        str
            What to submit in the ``COPY`` or ``UNLOAD`` job under CREDENTIALS
        """
        creds = self.session.get_credentials()
        if creds.token is not None:
            temp = "aws_access_key_id={};aws_secret_access_key={};token={}"
            return temp.format(creds.access_key, creds.secret_key, creds.token)
        else:
            temp = "aws_access_key_id={};aws_secret_access_key={}"
            return temp.format(creds.access_key, creds.secret_key)

    def _generate_s3_path(self, bucket, key):
        """Will return the S3 file URL in the format S3://bucket/key

        Parameters
        ----------
        bucket : str
            The AWS S3 bucket which you are copying the local file to.

        key : str
            The key to name the S3 object.

        Returns
        -------
        str
            string of the S3 file URL in the format S3://bucket/key
        """
        return "s3://{0}/{1}".format(bucket, key)

    def _generate_unload_path(self, bucket, folder):
        """Will return the S3 file URL in the format s3://bucket/folder if a
        valid (not None) folder is provided. Otherwise, returns s3://bucket

        Parameters
        ----------
        bucket : str
            The AWS S3 bucket which you are copying the local file to.

        folder : str
            The folder to unload files to. Note, if the folder does not end
            with a /, the file names will be prefixed with the folder arg.

        Returns
        -------
        str
            string of the S3 file URL in the format s3://bucket/folder
            If folder is None, returns format s3://bucket
        """
        if folder:
            s3_path = "s3://{0}/{1}".format(bucket, folder)
        else:
            s3_path = "s3://{0}".format(bucket)
        return s3_path

    def upload_to_s3(self, local, bucket, key):
        """
        Upload a file to a S3 bucket.

        Parameters
        ----------
        local : str
            The local file which you wish to copy.

        bucket : str
            The AWS S3 bucket which you are copying the local file to.

        key : str
            The key to name the S3 object.

        Raises
        ------
        S3UploadError
            If there is a issue uploading to the S3 bucket
        """
        extra_args = {}
        try:
            # force ServerSideEncryption
            if self.kms_key is None:
                extra_args["ServerSideEncryption"] = "AES256"
                logger.info("Using AES256 for encryption")
            else:
                extra_args["ServerSideEncryption"] = "aws:kms"
                extra_args["SSEKMSKeyId"] = self.kms_key
                logger.info("Using KMS Keys for encryption")

            logger.info("Uploading file to S3 bucket %s", self._generate_s3_path(bucket, key))
            self.s3.upload_file(
                local, bucket, key, ExtraArgs=extra_args, Callback=ProgressPercentage(local)
            )
        except Exception as e:
            logger.error("Error uploading to S3. err: %s", e)
            raise S3UploadError("Error uploading to S3.")

    def upload_list_to_s3(self, local_list, bucket, folder=None):
        """
        Upload a list of files to a S3 bucket.

        Parameters
        ----------
        local_list : list
            List of strings with the file paths of the files to upload

        bucket : str
            The AWS S3 bucket which you are copying the local file to.

        folder : str, optional
            The AWS S3 folder of the bucket which you are copying the local
            files to. Defaults to ``None``. Please note that you must follow the
            ``/`` convention when using subfolders.

        Returns
        -------
        list
            Returns a list of the generated S3 bucket and keys of the files which were uploaded. The
            ``S3://`` part is NOT include. The output would look like the following:
            ``["my-bucket/key1", "my-bucket/key2", ...]``

        Notes
        -----
        There is a assumption that if you are loading multiple files (via `splits`) it follows a
        structure such as `file_name.extension.#` (`#` splits). It allows for the `COPY` statement
        to use the key prefix vs specificing an exact file name. The returned list helps with this
        process downstream.
        """
        output = []
        for file in local_list:
            if folder is None:
                s3_key = os.path.basename(file)
            else:
                s3_key = "/".join([folder, os.path.basename(file)])
            self.upload_to_s3(file, bucket, s3_key)
            output.append("/".join([bucket, s3_key]))
        return output

    def download_from_s3(self, bucket, key, local):
        """
        Download a file from a S3 bucket.

        Parameters
        ----------
        bucket : str
            The AWS S3 bucket which you are copying the local file to.

        key : str
            The key to name the S3 object.

        local : str
            The local file which you wish to copy to.

        Raises
        ------
        S3DownloadError
            If there is a issue downloading to the S3 bucket
        """
        try:
            logger.info("Downloading file from S3 bucket %s", self._generate_s3_path(bucket, key))
            config = TransferConfig(max_concurrency=5)
            self.s3.download_file(bucket, key, local, Config=config)
        except Exception as e:
            logger.error("Error downloading from S3. err: %s", e)
            raise S3DownloadError("Error downloading from S3.")

    def download_list_from_s3(self, s3_list, local_path=None):
        """
        Download a list of files from s3.

        Parameters
        ----------
        s3_list : list
            List of strings with the s3 paths of the files to download

        local_path : str, optional
            The local path where the files will be copied to. Defualts to the current working
            directory (``os.getcwd()``)

        Returns
        -------
        list
            Returns a list of strings of the local file names
        """
        if local_path is None:
            local_path = os.getcwd()

        output = []
        for f in s3_list:
            s3_bucket, key = self.parse_s3_url(f)
            local = os.path.join(local_path, os.path.basename(key))
            self.download_from_s3(s3_bucket, key, local)
            output.append(local)
        return output

    def delete_from_s3(self, bucket, key):
        """
        Delete a file from an S3 bucket.

        Parameters
        ----------
        bucket : str
            The AWS S3 bucket from which you are deleting the file.

        key : str
            The name of the S3 object.

        Raises
        ------
        S3DeletionError
            If there is a issue deleting from the S3 bucket
        """
        try:
            logger.info("Deleting file from S3 bucket %s", self._generate_s3_path(bucket, key))
            self.s3.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            logger.error("Error deleting from S3. err: %s", e)
            raise S3DeletionError("Error deleting from S3.")

    def delete_list_from_s3(self, s3_list):
        """
        Delete a list of files from an S3 bucket.

        Parameters
        ----------
        s3_list : list
            List of strings with the s3 paths of the files to delete. The strings should not include
            the `s3://` scheme.
        """
        for file in s3_list:
            s3_bucket, s3_key = self.parse_s3_url(file)
            self.delete_from_s3(s3_bucket, s3_key)

    def parse_s3_url(self, s3_url):
        """
        Parse a string of the s3 url to extract the bucket and key.
        scheme or not.

        Parameters
        ----------
        s3_url : str
            s3 url. The string can include the `s3://` scheme (which is disgarded)

        Returns
        -------
        bucket: str
            s3 bucket
        key: str
            s3 key
        """
        temp_s3 = s3_url.replace("s3://", "")
        s3_bucket = temp_s3.split("/")[0]
        s3_key = "/".join(temp_s3.split("/")[1:])
        return s3_bucket, s3_key
