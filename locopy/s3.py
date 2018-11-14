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
            logger.info("Successfully initialized AWS session.")
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

    def _generate_s3_path(cls, bucket, key):
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

    def _generate_unload_path(cls, bucket, folder):
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
