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

"""Snowflake Module
Module to wrap a database adapter into a Snowflake class which can be used to connect
to Snowflake, and run arbitrary code.
"""
import os

from urllib.parse import urlparse
from .database import Database
from .s3 import S3
from .utility import ProgressPercentage, compress_file, split_file, write_file
from .logger import get_logger, DEBUG, INFO, WARN, ERROR, CRITICAL
from .errors import CredentialsError, DBError

logger = get_logger(__name__, INFO)


def combine_options(options):
    """ Returns the ``copy_options`` or ``format_options`` attribute with spaces in between and as
    a string. If options is ``None`` then return an empty string.

    Parameters
    ----------
    copy_options : list
        copy options which is to be converted into a single string with spaces
        inbetween.

    Returns
    -------
    str:
        ``options`` attribute with spaces in between
    """
    return " ".join(options) if options is not None else ""


class Snowflake(S3, Database):
    """Locopy class which manages connections to Snowflake.  Inherits ``Database`` and implements
    the specific ``COPY INTO`` functionality.

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
        A database adapter which is Python DB API 2.0 compliant (``snowflake.connector``)

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
        database adapter which is Python DBAPI 2.0 compliant (snowflake.connector)

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
        S3.__init__(self, profile, kms_key)
        Database.__init__(self, dbapi, config_yaml, **kwargs)

    def _connect(self):
        """Creates a connection to the Snowflake cluster by
        setting the values of the ``conn`` and ``cursor`` attributes.

        Raises
        ------
        DBError
            If there is a problem establishing a connection to Snowflake.
        """
        super(Snowflake, self)._connect()

        if self.connection.get("warehouse") is not None:
            self.execute("USE WAREHOUSE {}".format(self.connection["warehouse"]))
        if self.connection.get("database") is not None:
            self.execute("USE DATABASE {}".format(self.connection["database"]))

    def copy(
        self, tablename, stage, delim="|", header=False, format_options=None, copy_options=None
    ):
        """Executes the ``COPY INTO <table>`` command to load CSV files from a stage into
        a Snowflake table.

        Parameters
        ----------
        tablename : str
            The Snowflake table name which is being loaded. Must be fully qualified:
            `<namespace>.<table_name>`

        stage : str
            Stage location of the load file. This can be a internal or external stage

        delim : str
            The delimiter in a delimited file.

        header : bool, optional
            Boolean flag if header is included in the file

        format_options : list
            List of strings of format options to provide to the ``COPY INTO`` command.

        copy_options : list
            List of strings of copy options to provide to the ``COPY INTO`` command.

        Raises
        ------
        Exception
            If there is a problem executing the COPY command, a connection
            has not been initalized, or credentials are wrong.
        """
        if not self._is_connected():
            raise DBError("No Snowflake connection object is present.")

        format_options_text = combine_options(format_options)
        copy_options_text = combine_options(copy_options)
        base_copy_string = (
            "COPY INTO {0} FROM '{1}' "
            "FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER='{2}' SKIP_HEADER={3} {4}) {5}"
        )
        try:
            sql = base_copy_string.format(
                tablename, stage, delim, int(header), format_options_text, copy_options_text
            )
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error("Error running COPY on Snowflake. err: %s", e)
            raise DBError("Error running COPY on Snowflake.")

    def load_and_copy(
        self,
        local_file,
        s3_bucket,
        table_name,
        delim="|",
        format_options=None,
        copy_options=None,
        delete_s3_after=False,
        splits=1,
        compress=True,
        s3_folder=None,
    ):
        """Loads a file to S3, then copies into Snowflake.  Has options to
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
            The Snowflake table name which is being loaded

        delim : str, optional
            Delimiter for Snowflake ``COPY`` command. Defaults to ``|``

        format_options : list, optional
            A list (str) of format options that should be appended to the COPY
            statement. https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#formattypeoptions

        copy_options : list, optional
            A list (str) of copy options that should be appended to the COPY
            statement. https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#copyoptions

        delete_s3_after : bool, optional
            Lets you specify to delete the S3 file after transfer if you want.

        splits : int, optional
            Number of splits to perform for paralell loading into Snowflake.
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

        if format_options is None:
            format_options = []

        # generate the actual splitting of the files
        upload_list = split_file(local_file, local_file, splits=splits)

        if compress:
            format_options.append("COMPRESSION = GZIP")
            upload_list = compress_file_list(upload_list)

        # copy files to S3
        s3_upload_list = self.upload_list_to_s3(upload_list, s3_bucket, s3_folder)
        tmp_load_path = s3_upload_list[0].split(os.extsep)[0]

        # execute Snowflake COPY
        self.copy(
            table_name,
            "s3://" + tmp_load_path,
            delim,
            format_options=format_options,
            copy_options=copy_options,
        )

        # delete file from S3 (if set to do so)
        if delete_s3_after:
            for file in upload_list:
                if s3_folder is None:
                    s3_key = os.path.basename(file)
                else:
                    s3_key = "/".join([s3_folder, os.path.basename(file)])
                self.delete_from_s3(s3_bucket, s3_key)

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
        """``UNLOAD`` data from Snowflake, with options to write to a flat file,
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
            unload_options.append("PARALLEL OFF")

        ## run unload
        self.unload(query=query, s3path=s3path, unload_options=unload_options)

        ## parse unloaded files
        files = self._unload_generated_files()
        if files is None:
            logger.error("No files generated from unload")
            raise Exception("No files generated from unload")

        columns = self._get_column_names(query)
        if columns is None:
            logger.error("Unable to retrieve column names from exported data")
            raise Exception("Unable to retrieve column names from exported data.")

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
                with open(local, "rb") as temp_f:
                    with open(export_path, "ab") as main_f:
                        for line in temp_f:
                            main_f.write(line)
                os.remove(local)

        ## delete unloaded files from s3
        if delete_s3_after:
            for f in files:
                key = urlparse(f).path[1:]
                self.delete_from_s3(s3_bucket, key)

    def unload(self, query, s3path, unload_options=None):
        """Executes the UNLOAD command to export a query from
        Snowflake to S3.

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
            raise Exception("No Snowflake connection object is present")

        unload_options = unload_options or []
        unload_options_text = " ".join(unload_options)
        base_unload_string = "UNLOAD ('{0}')\n" "TO '{1}'\n" "CREDENTIALS '{2}'\n" "{3};"

        try:
            sql = base_unload_string.format(
                query.replace("'", r"\'"), s3path, self._credentials_string(), unload_options_text
            )
            self.execute(sql, commit=True)
        except Exception as e:
            logger.error("Error running UNLOAD on redshift. err: %s", e)
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
