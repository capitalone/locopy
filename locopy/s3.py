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

from urllib.parse import urlparse
from boto3 import Session
from boto3.s3.transfer import TransferConfig
from botocore.client import Config

from .locopy import Cmd
from .utility import ProgressPercentage, compress_file, split_file, write_file
from .logger import get_logger, DEBUG, INFO, WARN, ERROR, CRITICAL
from .errors import (S3Error, S3CredentialsError, S3InitializationError,
                     S3UploadError, S3DownloadError, S3DeletionError,
                     RedshiftConnectionError, RedshiftError)

logger = get_logger(__name__, DEBUG)


def add_default_copy_options(copy_options=None):
    """Adds in default options for the ``COPY`` job, unless those specific
    options have been provided in the request.

    Parameters
    ----------
    copy_options : list, optional
        List of copy options to be provided to the Redshift copy command

    Returns
    -------
    list
        list of strings with the default options appended. If ``copy_options``
        if not provided it will just return the default options.
    """
    if copy_options is None:
        copy_options = []
    default_options = ("DATEFORMAT 'auto'",
                       "COMPUPDATE ON",
                       "TRUNCATECOLUMNS")
    first_words = [opt.upper().split()[0] for opt in copy_options]

    for option in default_options:
        if option.split()[0] not in first_words:
            copy_options.append(option)
    return copy_options


def combine_copy_options(copy_options):
    """ Returns the ``copy_options`` attribute with spaces in between and as
    a string.

    Parameters
    ----------
    copy_options : list
        copy options which is to be converted into a single string with spaces
        inbetween.

    Returns
    -------
    str:
        ``copy_options`` attribute with spaces in between
    """
    return ' '.join(copy_options)


class S3(Cmd):
    """
    S3 wrapper class which utilizes the boto3 library to push files to an S3
    bucket.  Subclasses the Cmd class to inherit the Redshift connection.

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

    dbapi : DBAPI 2 module, optional
        A PostgreSQL database adapter which is Python DB API 2.0 compliant
        (``psycopg2``, ``pg8000``, etc.)

    host : str, optional
        Host name of the Redshift cluster to connect to.

    port : int, optional
        Port which connection will be made to Redshift.

    dbname : str, optional
        Redshift database name.

    user : str, optional
        Redshift users username.

    password : str, optional
        Redshift users password.

    config_yaml : str, optional
        String representing the file location of the credentials.

    s3_only : bool, optional
        If ``True`` then do not initialize the underlying redshift connection. It will
        allow users who want to soley interact with S3 to use that functionality.


    Attributes
    ----------
    session : boto3.Session
        Holds the session & credentials information for the S3 connection.

    kms_key : str
        String representing the s3 kms key

    session : boto3.Session
        Hold the AWS session credentials / info

    s3 : botocore.client.S3
        Hold the S3 client object which is used to upload/delete files to S3
    """
    def __init__(
        self, profile=None, kms_key=None, dbapi=None, host=None, port=None,
        dbname=None, user=None, password=None, config_yaml=None, s3_only=False, **kwargs):

        self.profile = profile
        self.kms_key = kms_key
        self.session = None
        self.s3 = None

        if not s3_only:
            super(S3, self).__init__(
                dbapi, host, port, dbname, user, password, config_yaml)

        self._set_session()
        self._set_client()


    def _set_session(self):
        try:
            self.session = Session(profile_name=self.profile)
            logger.info('Successfully initialized AWS session.')
        except Exception as e:
            logger.error('Error initializing AWS Session, err: %s', e)
            raise S3Error('Error initializing AWS Session.')
        credentials = self.session.get_credentials()
        if credentials is None:
            raise S3CredentialsError('Credentials could not be set.')


    def _set_client(self):
        try:
            self.s3 = self.session.client(
                's3', config=Config(signature_version='s3v4'))
            logger.info('Successfully initialized S3 client.')
        except Exception as e:
            logger.error('Error initializing S3 Client, err: %s', e)
            raise S3InitializationError('Error initializing S3 Client.')


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
            temp = 'aws_access_key_id={};aws_secret_access_key={};token={}'
            return temp.format(
                creds.access_key, creds.secret_key, creds.token)
        else:
            temp = 'aws_access_key_id={};aws_secret_access_key={}'
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
                extra_args['ServerSideEncryption'] = 'AES256'
                logger.info('Using AES256 for encryption')
            else:
                extra_args['ServerSideEncryption'] = 'aws:kms'
                extra_args['SSEKMSKeyId'] = self.kms_key
                logger.info('Using KMS Keys for encryption')

            logger.info('Uploading file to S3 bucket %s',
                        self._generate_s3_path(bucket, key))
            self.s3.upload_file(local, bucket, key,
                                ExtraArgs=extra_args,
                                Callback=ProgressPercentage(local))

        except Exception as e:
            logger.error('Error uploading to S3. err: %s', e)
            raise S3UploadError('Error uploading to S3.')


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
            logger.info('Downloading file from S3 bucket %s',
                        self._generate_s3_path(bucket, key))
            config = TransferConfig(max_concurrency=5)
            self.s3.download_file(bucket, key, local, Config=config)
        except Exception as e:
            logger.error('Error downloading from S3. err: %s', e)
            raise S3DownloadError('Error downloading from S3.')


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
            logger.info('Deleting file from S3 bucket %s',
                        self._generate_s3_path(bucket, key))
            self.s3.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            logger.error('Error deleting from S3. err: %s', e)
            raise S3DeletionError('Error deleting from S3.')


    def _copy_to_redshift(self, tablename, s3path, delim='|',
                         copy_options=None):
        """Executes the COPY command to load CSV files from S3 into
        a Redshift table.

        Parameters
        ----------
        tablename : str
            The Redshift table name which is being loaded

        s3path : str
            S3 path of the input file. eg: ``s3://path/to/file.csv``

        delim : str
            The delimiter in a delimited file.

        copy_options : list
            List of strings of copy options to provide to the ``COPY`` command.
            Will have default options added in.

        Raises
        ------
        Exception
            If there is a problem executing the COPY command, a connection
            has not been initalized, or credentials are wrong.
        """
        if not self._is_connected():
            raise RedshiftConnectionError(
                'No Redshift connection object is present.')

        copy_options = add_default_copy_options(copy_options)
        copy_options_text = combine_copy_options(copy_options)
        base_copy_string = ("COPY {0} FROM '{1}' "
                            "CREDENTIALS '{2}' "
                            "DELIMITER '{3}' {4};")
        try:
            sql = base_copy_string.format(
                tablename, s3path, self._credentials_string(), delim,
                copy_options_text)
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error('Error running COPY on Redshift. err: %s', e)
            raise RedshiftError('Error running COPY on Redshift.')


    def run_copy(self, local_file, s3_bucket, table_name, delim="|",
                 copy_options=None, delete_s3_after=False, splits=1,
                 compress=True, s3_folder=None):
        """Loads a file to S3, then copies into Redshift.  Has options to
        split a single file into multiple files, compress using gzip, and
        upload to an S3 bucket with folders within the bucket.

        Notes
        -----
        If you are using folders in your S3 bucket please be aware of having
        special chars or backward slashes (``\``). These may cause the file to
        upload but fail on the ``COPY`` command.

        Parameters
        ----------
        local_file : str
            The local file which you wish to copy.

        s3_bucket : str
            The AWS S3 bucket which you are copying the local file to.

        table_name : str
            The Redshift table name which is being loaded

        delim : str, optional
            Delimiter for Redshift ``COPY`` command. Defaults to ``|``

        copy_options : list, optional
            A list (str) of copy options that should be appended to the COPY
            statement.  The class will insert a default for DATEFORMAT,
            COMPUPDATE and TRUNCATECOLUMNS if they are not provided in this
            list. See http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html
            for options which could be passed.

        delete_s3_after : bool, optional
            Lets you specify to delete the S3 file after transfer if you want.

        splits : int, optional
            Number of splits to perform for paralell loading into Redshift.
            Must be greater than ``0``. Recommended that this number should be
            less than ``100``. Defaults to ``1``.

        compress : bool, optional
            Whether to compress the output file with ``gzip`` or leave it raw.
            Defaults to ``True``

        s3_folder : str, optional
            The AWS S3 folder of the bucket which you are copying the local
            file to. Defaults to ``None``. Please note that you must follow the
            ``/`` convention when using subfolders.
        """

        if copy_options is None:
            copy_options = []

        # generate the actual splitting of the files
        if splits > 1:
            upload_list = split_file(local_file, local_file, splits=splits)
        else:
            upload_list = [local_file]

        if compress:
            copy_options.append("GZIP")
            for i, f in enumerate(upload_list):
                gz = '{0}.gz'.format(f)
                compress_file(f, gz)
                upload_list[i] = gz
                os.remove(f)  # cleanup old files

        # copy file to S3
        for file in upload_list:
            if s3_folder is None:
                s3_key = os.path.basename(file)
            else:
                s3_key = '/'.join([s3_folder, os.path.basename(file)])

            self.upload_to_s3(file, s3_bucket, s3_key)

        # execute Redshift COPY
        self._copy_to_redshift(
            table_name, self._generate_s3_path(
                s3_bucket,
                s3_key.split(os.extsep)[0]),
            delim,
            copy_options=copy_options)

        # delete file from S3 (if set to do so)
        if delete_s3_after:
            for file in upload_list:
                if s3_folder is None:
                    s3_key = os.path.basename(file)
                else:
                    s3_key = '/'.join([s3_folder, os.path.basename(file)])
                self.delete_from_s3(s3_bucket, s3_key)


    def run_unload(self, query, s3_bucket, s3_folder=None, export_path=False,
                   delimiter=',', delete_s3_after=True,
                   parallel_off=False, unload_options=None):
        """``UNLOAD`` data from Redshift, with options to write to a flat file,
        and store on S3.

        Parameters
        ----------
        query : str
            A query to be unloaded to S3. Typically a ``SELECT`` statement

        s3_bucket : str
            The AWS S3 bucket where the data from the query will be unloaded.

        s3_folder : str, optional
            The AWS S3 folder of the bucket where the data from the query will
            be unloaded. Defaults to ``None``. Please note that you must follow
            the ``/`` convention when using subfolders.

        export_path : str, optional
            If a ``export_path`` is provided, function will write the unloaded
            files to that folder.

        delimiter : str, optional
            Delimiter for unloading and file writing. Defaults to a comma.

        delete_s3_after : bool, optional
            Delete the files from S3 after unloading. Defaults to True.

        parallel_off : bool, optional
            Unload data to S3 as a single file. Defaults to False.
            Not recommended as it will decrease speed.

        unload_options : list, optional
            A list of unload options that should be appended to the UNLOAD
            statement.

        Raises
        ------
        Exception
            If no files are generated from the unload.
            If the column names from the query cannot be retrieved.
            If there is a issue with the execution of any of the queries.
        """
        # data = []
        s3path = self._generate_unload_path(s3_bucket, s3_folder)

        ## configure unload options
        if unload_options is None:
            unload_options = []

        unload_options.append("DELIMITER '{0}'".format(delimiter))
        if parallel_off:
            unload_options.append('PARALLEL OFF')

        ## run unload
        self._unload_to_s3(query=query, s3path=s3path,
                           unload_options=unload_options)

        ## parse unloaded files
        files = self._unload_generated_files()
        if files is None:
            logger.error('No files generated from unload')
            raise Exception('No files generated from unload')

        columns = self._get_column_names(query)
        if columns is None:
            logger.error('Unable to retrieve column names from exported data')
            raise Exception(
                    'Unable to retrieve column names from exported data.')

        # download files locally with same name
        # write columns to local file
        # write temp to local file
        # remove temp files
        if export_path:
            write_file([columns], delimiter, export_path)
            for f in files:
                key = urlparse(f).path[1:]
                local = os.path.basename(key)
                self.download_from_s3(s3_bucket, key, local)
                with open(local, 'rb') as temp_f:
                    with open(export_path, 'ab') as main_f:
                        for line in temp_f:
                            main_f.write(line)
                os.remove(local)

        ## delete unloaded files from s3
        if delete_s3_after:
            for f in files:
                key = urlparse(f).path[1:]
                self.delete_from_s3(s3_bucket, key)


    def _unload_to_s3(self, query, s3path, unload_options=None):
        """Executes the UNLOAD command to export a query from
        Redshift to S3.

        Parameters
        ----------
        query : str
            A query to be unloaded to S3.

        s3path : str
            S3 path for the output files.

        unload_options : list
            List of string unload options.

        Raises
        ------
        Exception
            If there is a problem executing the unload command.
        """
        if not self._is_connected():
            raise Exception("No Redshift connection object is present")

        unload_options = unload_options or []
        unload_options_text = ' '.join(unload_options)
        base_unload_string = (
            "UNLOAD ('{0}')\n"
            "TO '{1}'\n"
            "CREDENTIALS '{2}'\n"
            "{3};")

        try:
            sql = base_unload_string.format(
                query.replace("'", r"\'"), s3path, self._credentials_string(),
                unload_options_text)
            self.execute(sql, commit=True)
        except Exception as e:
            logger.error('Error running UNLOAD on redshift. err: %s', e)
            raise


    def _get_column_names(self, query):
        """Gets a list of column names from the supplied query.

        Parameters
        ----------
        query : str
            A query (or table name) to be unloaded to S3.

        Returns
        -------
        list
            List of column names. Returns None if no columns were retrieved.
        """

        try:
            logger.info('Retrieving column names')
            sql = "SELECT * FROM ({}) WHERE 1 = 0".format(query)
            self.execute(sql)
            results = [desc for desc in self.cursor.description]
            if len(results) > 0:
                return [result[0].strip() for result in results]
            else:
                return None
        except Exception as e:
            logger.error('Error retrieving column names')
            raise


    def _unload_generated_files(self):
        """Gets a list of files generated by the unload process

        Returns
        -------
        list
            List of S3 file names
        """
        sql = (
            'SELECT path FROM stl_unload_log '
            'WHERE query = pg_last_query_id() ORDER BY path')
        try:
            logger.info('Getting list of unloaded files')
            self.execute(sql)
            results = self.cursor.fetchall()
            if len(results) > 0:
                return [result[0].strip() for result in results]
            else:
                return None
        except Exception as e:
            logger.error('Error retrieving unloads generated files')
            raise
