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

"""Redshift Module
Module to wrap a database adapter into a Redshift class which can be used to connect
to Redshift, and run arbitrary code.
"""
import os

from .logger import logger
from .database import Database
from .s3 import S3
from .utility import (
    ProgressPercentage,
    compress_file_list,
    split_file,
    write_file,
    concatenate_files,
)
from .errors import CredentialsError, DBError


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
    default_options = ("DATEFORMAT 'auto'", "COMPUPDATE ON", "TRUNCATECOLUMNS")
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
    return " ".join(copy_options)


class Redshift(S3, Database):
    """Locopy class which manages connections to Redshift.  Inherits ``Database`` and implements the
    specific ``COPY`` and ``UNLOAD`` functionality.

    If any of host, port, dbname, user and password are not provided, a config_yaml file must be
    provided with those parameters in it. Please note ssl is always enforced when connecting.

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
        A database adapter which is Python DB API 2.0 compliant
        (``psycopg2``, ``pg8000``, etc.)

    config_yaml : str, optional
        String representing the YAML file location of the database connection keyword arguments. It
        is worth noting that this should only contain valid arguments for the database connector you
        plan on using. It will throw an exception if something is passed through which isn't valid.

    **kwargs
        Database connection keyword arguments.

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

    dbapi : DBAPI 2 module
        database adapter which is Python DBAPI 2.0 compliant

    connection : dict
        Dictionary of database connection items

    conn : dbapi.connection
        DBAPI connection instance

    cursor : dbapi.cursor
        DBAPI cursor instance

    Raises
    ------
    CredentialsError
        Database credentials are not provided or valid

    S3Error
        Error initializing AWS Session (ex: invalid profile)

    S3CredentialsError
        Issue with AWS credentials

    S3InitializationError
        Issue initializing S3 session
    """

    def __init__(self, profile=None, kms_key=None, dbapi=None, config_yaml=None, **kwargs):
        try:
            S3.__init__(self, profile, kms_key)
        except S3CredentialsError:
            logger.warning("S3 credentials we not found. S3 functionality is disabled")
            logger.warning("Only internal stages are available")
        Database.__init__(self, dbapi, config_yaml, **kwargs)

    def _connect(self):
        """Creates a connection to the Redshift cluster by
        setting the values of the ``conn`` and ``cursor`` attributes.

        Raises
        ------
        DBError
            If there is a problem establishing a connection to Redshift.
        """
        if self.dbapi.__name__ == "psycopg2":
            self.connection["sslmode"] = "require"
        elif self.dbapi.__name__ == "pg8000":
            self.connection["ssl"] = True
        super(Redshift, self)._connect()

    def copy(self, table_name, s3path, delim="|", copy_options=None):
        """Executes the COPY command to load delimited files from S3 into
        a Redshift table.

        Parameters
        ----------
        table_name : str
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
        DBError
            If there is a problem executing the COPY command, a connection
            has not been initalized, or credentials are wrong.
        """
        if not self._is_connected():
            raise DBError("No Redshift connection object is present.")

        copy_options = add_default_copy_options(copy_options)
        copy_options_text = combine_copy_options(copy_options)
        base_copy_string = "COPY {0} FROM '{1}' " "CREDENTIALS '{2}' " "DELIMITER '{3}' {4};"
        try:
            sql = base_copy_string.format(
                table_name, s3path, self._credentials_string(), delim, copy_options_text
            )
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error("Error running COPY on Redshift. err: {err}", err=e)
            raise DBError("Error running COPY on Redshift.")

    def load_and_copy(
        self,
        local_file,
        s3_bucket,
        table_name,
        delim="|",
        copy_options=None,
        delete_s3_after=False,
        splits=1,
        compress=True,
        s3_folder=None,
    ):
        """Loads a file to S3, then copies into Redshift.  Has options to
        split a single file into multiple files, compress using gzip, and
        upload to an S3 bucket with folders within the bucket.

        Notes
        -----
        If you are using folders in your S3 bucket please be aware of having
        special chars or backward slashes (``\``). These may cause the file to
        upload but fail on the ``COPY`` command.

        By default `locopy` will handle the splitting of files for you, in order to reduce
        complexity in uploading to s3 and generating the `COPY` command.

        It is critical to ensure that the S3 location you are using that it only contains the files
        you want to load. In the case of a "folder" it should only contain the files you want to
        load. For a bucket the file name should be unique enough as any extensions get striped out
        in favour of the file prefix.

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
        upload_list = split_file(local_file, local_file, splits=splits)

        if compress:
            copy_options.append("GZIP")
            upload_list = compress_file_list(upload_list)

        # copy files to S3
        s3_upload_list = self.upload_list_to_s3(upload_list, s3_bucket, s3_folder)
        tmp_load_path = s3_upload_list[0].split(os.extsep)[0]

        # execute Redshift COPY
        self.copy(table_name, "s3://" + tmp_load_path, delim, copy_options=copy_options)

        # delete file from S3 (if set to do so)
        if delete_s3_after:
            self.delete_list_from_s3(s3_upload_list)

    def unload_and_copy(
        self,
        query,
        s3_bucket,
        s3_folder=None,
        export_path=False,
        delimiter=",",
        delete_s3_after=True,
        parallel_off=False,
        unload_options=None,
    ):
        """``UNLOAD`` data from Redshift, with options to write to a flat file,
        and store on S3.

        Parameters
        ----------
        query : str
            A query to be unloaded to S3. A ``SELECT`` query

        s3_bucket : str
            The AWS S3 bucket where the data from the query will be unloaded.

        s3_folder : str, optional
            The AWS S3 folder of the bucket where the data from the query will
            be unloaded. Defaults to ``None``. Please note that you must follow
            the ``/`` convention when using subfolders.

        export_path : str, optional
            If a ``export_path`` is provided, function will write the unloaded
            files to this path as a single file. If your file is very large you may not want to
            use this option.

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
            unload_options.append("PARALLEL OFF")

        ## run unload
        self.unload(query=query, s3path=s3path, unload_options=unload_options)

        ## parse unloaded files
        s3_download_list = self._unload_generated_files()
        if s3_download_list is None:
            logger.error("No files generated from unload")
            raise Exception("No files generated from unload")

        columns = self._get_column_names(query)
        if columns is None:
            logger.error("Unable to retrieve column names from exported data")
            raise Exception("Unable to retrieve column names from exported data.")

        # download files locally with same name
        local_list = self.download_list_from_s3(s3_download_list)
        if export_path:
            write_file([columns], delimiter, export_path)  # column
            concatenate_files(local_list, export_path)  # data

        # delete unloaded files from s3
        if delete_s3_after:
            self.delete_list_from_s3(s3_download_list)

    def unload(self, query, s3path, unload_options=None):
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
        DBError
            If there is a problem executing the UNLOAD command, a connection
            has not been initalized, or credentials are wrong.
        """
        if not self._is_connected():
            raise DBError("No Redshift connection object is present")

        unload_options = unload_options or []
        unload_options_text = " ".join(unload_options)
        base_unload_string = "UNLOAD ('{0}')\n" "TO '{1}'\n" "CREDENTIALS '{2}'\n" "{3};"

        try:
            sql = base_unload_string.format(
                query.replace("'", r"\'"), s3path, self._credentials_string(), unload_options_text
            )
            self.execute(sql, commit=True)
        except Exception as e:
            logger.error("Error running UNLOAD on redshift. err: {err}", err=e)
            raise DBError("Error running UNLOAD on redshift.")

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
            logger.info("Retrieving column names")
            sql = "SELECT * FROM ({}) WHERE 1 = 0".format(query)
            self.execute(sql)
            results = [desc for desc in self.cursor.description]
            if len(results) > 0:
                return [result[0].strip() for result in results]
            else:
                return None
        except Exception as e:
            logger.error("Error retrieving column names")
            raise

    def _unload_generated_files(self):
        """Gets a list of files generated by the unload process

        Returns
        -------
        list
            List of S3 file names
        """
        sql = "SELECT path FROM stl_unload_log " "WHERE query = pg_last_query_id() ORDER BY path"
        try:
            logger.info("Getting list of unloaded files")
            self.execute(sql)
            results = self.cursor.fetchall()
            if len(results) > 0:
                return [result[0].strip() for result in results]
            else:
                return None
        except Exception as e:
            logger.error("Error retrieving unloads generated files")
            raise
