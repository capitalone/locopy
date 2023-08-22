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
from pathlib import PurePath

from .database import Database
from .errors import DBError, S3CredentialsError
from .logger import INFO, get_logger
from .s3 import S3
from .utility import find_column_type

logger = get_logger(__name__, INFO)

COPY_FORMAT_OPTIONS = {
    "csv": {
        "compression",
        "record_delimiter",
        "field_delimiter",
        "skip_header",
        "date_format",
        "time_format",
        "timestamp_format",
        "binary_format",
        "escape",
        "escape_unenclosed_field",
        "trim_space",
        "field_optionally_enclosed_by",
        "null_if",
        "error_on_column_count_mismatch",
        "validate_utf8",
        "empty_field_as_null",
        "skip_byte_order_mark",
        "encoding",
    },
    "json": {
        "compression",
        "file_extension",
        "enable_octal",
        "allow_duplicate",
        "strip_outer_array",
        "strip_null_values",
        "ignore_utf8_errors",
        "skip_byte_order_mark",
    },
    "parquet": {"binary_as_text"},
}


UNLOAD_FORMAT_OPTIONS = {
    "csv": {
        "compression",
        "record_delimiter",
        "field_delimiter",
        "file_extension",
        "date_format",
        "time_format",
        "timestamp_format",
        "binary_format",
        "escape",
        "escape_unenclosed_field",
        "field_optionally_enclosed_by",
        "null_if",
    },
    "json": {"compression", "file_extension"},
    "parquet": {"snappy_compression"},
}


def combine_options(options=None):
    """Returns the ``copy_options`` or ``format_options`` attribute with spaces in between and as
    a string. If options is ``None`` then return an empty string.

    Parameters
    ----------
    options : list, optional
        list of strings which is to be converted into a single string with spaces
        inbetween. Defaults to ``None``

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
        The name of the AWS profile to use which is typically stored in the
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

    def __init__(
        self, profile=None, kms_key=None, dbapi=None, config_yaml=None, **kwargs
    ):
        try:
            S3.__init__(self, profile, kms_key)
        except S3CredentialsError:
            logger.warning(
                "S3 credentials were not found. S3 functionality is disabled"
            )
            logger.warning("Only internal stages are available")
        Database.__init__(self, dbapi, config_yaml, **kwargs)

    def connect(self):
        """Creates a connection to the Snowflake cluster by
        setting the values of the ``conn`` and ``cursor`` attributes.

        Raises
        ------
        DBError
            If there is a problem establishing a connection to Snowflake.
        """
        super(Snowflake, self).connect()

        if self.connection.get("warehouse") is not None:
            self.execute("USE WAREHOUSE {0}".format(self.connection["warehouse"]))
        if self.connection.get("database") is not None:
            self.execute("USE DATABASE {0}".format(self.connection["database"]))
        if self.connection.get("schema") is not None:
            self.execute("USE SCHEMA {0}".format(self.connection["schema"]))

    def upload_to_internal(
        self, local, stage, parallel=4, auto_compress=True, overwrite=True
    ):
        """
        Upload file(s) to a internal stage via the ``PUT`` command.

        Parameters
        ----------
        local : str
            The local directory path to the file to upload. Wildcard characters (``*``, ``?``) are
            supported to enable uploading multiple files in a directory. Otherwise it must be the
            absolute path.

        stage : str
            Internal stage location to load the file.

        parallel : int, optional
            Specifies the number of threads to use for uploading files.

        auto_compress : bool, optional
            Specifies if Snowflake uses gzip to compress files during upload.
            If ``True``, the files are compressed (if they are not already compressed).
            if ``False``, the files are uploaded as-is.

        overwrite : bool, optional
            Specifies whether Snowflake overwrites an existing file with the same name during upload.
            If ``True``, existing file with the same name is overwritten.
            if ``False``, existing file with the same name is not overwritten.
        """
        local_uri = PurePath(local).as_posix()
        self.execute(
            "PUT 'file://{0}' {1} PARALLEL={2} AUTO_COMPRESS={3} OVERWRITE={4}".format(
                local_uri, stage, parallel, auto_compress, overwrite
            )
        )

    def download_from_internal(self, stage, local=None, parallel=10):
        """
        Download file(s) from a internal stage via the ``GET`` command.

        Parameters
        ----------
        stage : str
            Internal stage location to load the file.

        local : str, optional
            The local directory path where files will be downloaded to. Defualts to the current
            working directory (``os.getcwd()``). Otherwise it must be the absolute path.

        parallel : int, optional
            Specifies the number of threads to use for downloading files.
        """
        if local is None:
            local = os.getcwd()
        local_uri = PurePath(local).as_posix()
        self.execute(
            "GET {0} 'file://{1}' PARALLEL={2}".format(stage, local_uri, parallel)
        )

    def copy(
        self, table_name, stage, file_type="csv", format_options=None, copy_options=None
    ):
        """Executes the ``COPY INTO <table>`` command to load CSV files from a stage into
        a Snowflake table. If ``file_type == csv`` and ``format_options == None``, ``format_options``
        will default to: ``["FIELD_DELIMITER='|'", "SKIP_HEADER=0"]``

        Parameters
        ----------
        table_name : str
            The Snowflake table name which is being loaded. Must be fully qualified:
            `<namespace>.<table_name>`

        stage : str
            Stage location of the load file. This can be a internal or external stage

        file_type : str
            The file type. One of ``csv``, ``json``, or ``parquet``

        format_options : list
            List of strings of format options to provide to the ``COPY INTO`` command. The options
            will typically be in the format of ``["a=b", "c=d"]``

        copy_options : list
            List of strings of copy options to provide to the ``COPY INTO`` command.

        Raises
        ------
        DBError
            If there is a problem executing the COPY command, or a connection
            has not been initalized.
        """
        if not self._is_connected():
            raise DBError("No Snowflake connection object is present.")

        if file_type not in COPY_FORMAT_OPTIONS:
            raise ValueError(
                "Invalid file_type. Must be one of {0}".format(
                    list(COPY_FORMAT_OPTIONS.keys())
                )
            )

        if format_options is None and file_type == "csv":
            format_options = ["FIELD_DELIMITER='|'", "SKIP_HEADER=0"]

        format_options_text = combine_options(format_options)
        copy_options_text = combine_options(copy_options)
        base_copy_string = (
            "COPY INTO {0} FROM '{1}' " "FILE_FORMAT = (TYPE='{2}' {3}) {4}"
        )
        try:
            sql = base_copy_string.format(
                table_name, stage, file_type, format_options_text, copy_options_text
            )
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error("Error running COPY on Snowflake. err: %s", e)
            raise DBError("Error running COPY on Snowflake.")

    def unload(
        self,
        stage,
        table_name,
        file_type="csv",
        format_options=None,
        header=False,
        copy_options=None,
    ):
        """Executes the ``COPY INTO <location>`` command to export a query/table from
        Snowflake to a stage. If ``file_type == csv`` and ``format_options == None``, ``format_options``
        will default to: ``["FIELD_DELIMITER='|'"]``

        Parameters
        ----------
        stage : str
            Stage location (internal or external) where the data files are unloaded

        table_name : str
            The Snowflake table name which is being unloaded. Must be fully qualified:
            ``<namespace>.<table_name>``

        file_type : str
            The file type. One of ``csv``, ``json``, or ``parquet``

        format_options : list
            List of strings of format options to provide to the ``COPY INTO`` command.

        header : bool, optional
            Boolean flag if header is included in the file(s)

        copy_options : list
            List of strings of copy options to provide to the ``COPY INTO`` command.

        Raises
        ------
        DBError
            If there is a problem executing the UNLOAD command, or a connection
            has not been initalized.
        """
        if not self._is_connected():
            raise DBError("No Snowflake connection object is present")

        if file_type not in COPY_FORMAT_OPTIONS:
            raise ValueError(
                "Invalid file_type. Must be one of {0}".format(
                    list(UNLOAD_FORMAT_OPTIONS.keys())
                )
            )

        if format_options is None and file_type == "csv":
            format_options = ["FIELD_DELIMITER='|'"]

        format_options_text = combine_options(format_options)
        copy_options_text = combine_options(copy_options)
        base_unload_string = (
            "COPY INTO {0} FROM {1} " "FILE_FORMAT = (TYPE='{2}' {3}) HEADER={4} {5}"
        )

        try:
            sql = base_unload_string.format(
                stage,
                table_name,
                file_type,
                format_options_text,
                header,
                copy_options_text,
            )
            self.execute(sql, commit=True)
        except Exception as e:
            logger.error("Error running UNLOAD on Snowflake. err: %s", e)
            raise DBError("Error running UNLOAD on Snowflake.")

    def insert_dataframe_to_table(
        self, dataframe, table_name, columns=None, create=False, metadata=None
    ):
        """
        Insert a Pandas dataframe to an existing table or a new table. In newer versions of the
        python snowflake connector (v2.1.2+) users can call the ``write_pandas`` method from the cursor
        directly, ``insert_dataframe_to_table`` is a custom implementation and does not use
        ``write_pandas``. Instead of using ``COPY INTO`` the method builds a list of tuples to
        insert directly into the table. There are also options to create the table if it doesn't
        exist and use your own metadata. If your data is significantly large then using
        ``COPY INTO <table>`` is more appropriate.

        Parameters
        ----------
        dataframe: Pandas Dataframe
            The pandas dataframe which needs to be inserted.

        table_name: str
            The name of the Snowflake table which is being inserted.

        columns: list, optional
            The list of columns which will be uploaded.

        create: bool, optional
            Boolean flag if a new table need to be created and insert to.

        metadata: dictionary, optional
            If metadata==None, it will be generated based on data
        """

        import pandas as pd

        if columns:
            dataframe = dataframe[columns]

        all_columns = columns or list(dataframe.columns)
        column_sql = "(" + ",".join(all_columns) + ")"
        string_join = "(" + ",".join(["%s"] * len(all_columns)) + ")"

        # create a list of tuples for insert
        to_insert = []
        for row in dataframe.itertuples(index=False):
            none_row = tuple([None if pd.isnull(val) else str(val) for val in row])
            to_insert.append(none_row)

        if not create and metadata:
            logger.warning("Metadata will not be used because create is set to False.")

        if create:
            if not metadata:
                logger.info("Metadata is missing. Generating metadata ...")
                metadata = find_column_type(dataframe, "snowflake")
                logger.info("Metadata is complete. Creating new table ...")

            create_join = (
                "("
                + ",".join(
                    [
                        list(metadata.keys())[i] + " " + list(metadata.values())[i]
                        for i in range(len(metadata))
                    ]
                )
                + ")"
            )
            column_sql = "(" + ",".join(list(metadata.keys())) + ")"
            create_query = "CREATE TABLE {table_name} {create_join}".format(
                table_name=table_name, create_join=create_join
            )
            self.execute(create_query)
            logger.info("New table has been created")

        insert_query = """INSERT INTO {table_name} {columns} VALUES {values}""".format(
            table_name=table_name, columns=column_sql, values=string_join
        )

        logger.info("Inserting records...")
        self.execute(insert_query, params=to_insert, many=True)
        logger.info("Table insertion has completed")

    def to_dataframe(self, size=None):
        """Return a dataframe of the last query results. This is just a convenience method. This
        method overrides the base classes implementation in favour for the snowflake connectors
        built-in ``fetch_pandas_all`` when ``size==None``. If ``size != None`` then we will continue
        to use the existing functionality where we iterate through the cursor and build the
        dataframe.

        Parameters
        ----------
        size : int, optional
            Chunk size to fetch.  Defaults to None.

        Returns
        -------
        pandas.DataFrame
            Dataframe with lowercase column names.  Returns None if no fetched
            result.
        """
        if size is None and self.cursor._query_result_format == "arrow":
            return self.cursor.fetch_pandas_all()
        else:
            return super(Snowflake, self).to_dataframe(size)
