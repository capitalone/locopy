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

"""Redshift Module.

Module to wrap a database adapter into a Redshift class which can be used to connect
to Redshift, and run arbitrary code.
"""

import os
from functools import singledispatch
from pathlib import Path

import pandas as pd
import polars as pl
import polars.selectors as cs

from locopy.postgres_base import PostgresBase
from locopy.errors import DBError, S3CredentialsError
from locopy.logger import INFO, get_logger
from locopy.s3 import S3
from locopy.utility import (
    compress_file_list,
    concatenate_files,
    find_column_type,
    get_ignoreheader_number,
    split_file,
    write_file,
)

logger = get_logger(__name__, INFO)


def add_default_copy_options(copy_options=None):
    """Add in default options for the ``COPY`` job.

    Unless those specific options have been provided in the request.

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
    """Return the ``copy_options`` attribute with spaces in between.

    Converts to a string.

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


class Redshift(S3, PostgresBase):
    """Locopy class which manages connections to Redshift.

    Inherits ``PostgresBase`` and implements the specific ``COPY`` and ``UNLOAD`` functionality.

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

    def __init__(
        self, profile=None, kms_key=None, dbapi=None, config_yaml=None, **kwargs
    ):
        try:
            S3.__init__(self, profile, kms_key)
        except S3CredentialsError:
            logger.warning(
                "S3 credentials were not found. S3 functionality is disabled"
            )
        PostgresBase.__init__(self, dbapi, config_yaml, **kwargs)

    def connect(self):
        """Create a connection to the Redshift cluster.

        Sets the values of the ``conn`` and ``cursor`` attributes.

        Raises
        ------
        DBError
            If there is a problem establishing a connection to Redshift.
        """
        if self.dbapi.__name__ == "psycopg2":
            self.connection["sslmode"] = "require"
        elif self.dbapi.__name__ == "pg8000":
            self.connection["ssl_context"] = True
        super().connect()

    def copy(self, table_name, s3path, delim="|", copy_options=None):
        """Execute the COPY command to load files from S3 into a Redshift table.

        Parameters
        ----------
        table_name : str
            The Redshift table name which is being loaded

        s3path : str
            S3 path of the input file. eg: ``s3://path/to/file.csv``

        delim : str, optional
           None for non-delimited file type. Defaults to |

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
        if (copy_options and "PARQUET" not in copy_options) or copy_options is None:
            copy_options = add_default_copy_options(copy_options)
        if delim:
            copy_options = [f"DELIMITER '{delim}'", *copy_options]
        copy_options_text = combine_copy_options(copy_options)
        base_copy_string = "COPY {0} FROM '{1}' " "CREDENTIALS '{2}' " "{3};"
        try:
            sql = base_copy_string.format(
                table_name, s3path, self._credentials_string(), copy_options_text
            )
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error("Error running COPY on Redshift. err: %s", e)
            raise DBError("Error running COPY on Redshift.") from e

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
        """Load a file to S3, then copies into Redshift.

        Has options to split a single file into multiple files, compress using gzip, and
        upload to an S3 bucket with folders within the bucket.

        Parameters
        ----------
        local_file : str
            Path to the file that is to be loaded.

        s3_bucket : str
            Target S3 bucket to upload to.

        table_name : str
            The Redshift table name which is being loaded.

        delim : str, optional
            Delimiter to be used in the ``COPY`` command. Defaults to |.

        copy_options : list, optional
            List of strings of copy options to provide to the ``COPY`` command.
            Will have default options added in.

        delete_s3_after : bool, optional
            Whether to delete the files from S3 after upload.

        splits : int, optional
            Number of splits to perform for parallel loading into Redshift.

        compress : bool, optional
            Whether to compress the file before uploading to S3.

        s3_folder : str, optional
            Folder to upload to within the S3 bucket.

        Raises
        ------
        DBError
            If there is a problem executing the COPY command, a connection
            has not been initalized, or credentials are wrong.
        """
        # split the file
        if splits > 1:
            split_files = split_file(local_file, splits)
        else:
            split_files = [local_file]

        # compress if specified
        if compress:
            split_files = compress_file_list(split_files)

        # upload files to S3
        s3_paths = []
        for file in split_files:
            s3_path = self.upload_to_s3(file, s3_bucket, s3_folder)
            s3_paths.append(s3_path)

        # copy into redshift
        self.copy(table_name, s3_paths[0], delim, copy_options)

        # delete from S3 if specified
        if delete_s3_after:
            for s3_path in s3_paths:
                self.delete_from_s3(s3_path)

    def unload(self, query, s3path, unload_options=None):
        """Execute the UNLOAD command to export data from Redshift to S3.

        Parameters
        ----------
        query : str
            The query to be unloaded.

        s3path : str
            S3 path to write the unloaded files to.

        unload_options : list, optional
            List of strings of unload options to provide to the ``UNLOAD`` command.

        Raises
        ------
        DBError
            If there is a problem executing the UNLOAD command, a connection
            has not been initalized, or credentials are wrong.
        """
        if not self._is_connected():
            raise DBError("No Redshift connection object is present.")

        if unload_options is None:
            unload_options = []

        unload_options_text = " ".join(unload_options)
        base_unload_string = (
            "UNLOAD ('{0}') TO '{1}' "
            "CREDENTIALS '{2}' "
            "{3};"
        )
        try:
            sql = base_unload_string.format(
                query, s3path, self._credentials_string(), unload_options_text
            )
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error("Error running UNLOAD on Redshift. err: %s", e)
            raise DBError("Error running UNLOAD on Redshift.") from e

    def unload_and_copy(
        self,
        query,
        s3_bucket,
        s3_folder=None,
        raw_unload_path=None,
        export_path=False,
        delim=",",
        delete_s3_after=True,
        parallel_off=False,
        unload_options=None,
    ):
        """Unload data from Redshift into S3, with options to save locally.

        Parameters
        ----------
        query : str
            The query to be unloaded.

        s3_bucket : str
            S3 bucket to upload to.

        s3_folder : str, optional
            Folder to upload to within the S3 bucket.

        raw_unload_path : str, optional
            Path to save the raw unloaded files to.

        export_path : str or bool, optional
            Path to save the concatenated data to. If True, uses the
            raw_unload_path as the export path.

        delim : str, optional
            Delimiter to be used in the ``UNLOAD`` command. Defaults to ,.

        delete_s3_after : bool, optional
            Whether to delete the files from S3 after download.

        parallel_off : bool, optional
            Whether to disable parallel unloading.

        unload_options : list, optional
            List of strings of unload options to provide to the ``UNLOAD`` command.

        Returns
        -------
        list
            List of files that were downloaded from S3.

        Raises
        ------
        DBError
            If there is a problem executing the UNLOAD command, a connection
            has not been initalized, or credentials are wrong.
        """
        if unload_options is None:
            unload_options = []

        # add default options
        if parallel_off:
            unload_options.append("PARALLEL OFF")
        unload_options.extend([
            "HEADER",
            "ADDQUOTES",
            f"DELIMITER '{delim}'",
            "ESCAPE",
            "GZIP"
        ])

        # generate s3 path
        s3_path = f"s3://{s3_bucket}"
        if s3_folder:
            s3_path = f"{s3_path}/{s3_folder}"
        s3_path = f"{s3_path}/unload_"

        # unload to S3
        self.unload(query, s3_path, unload_options)

        # download files if requested
        downloaded_files = []
        if raw_unload_path or export_path:
            # list files in S3
            s3_files = self.list_bucket(s3_bucket, s3_folder)
            
            # download each file
            for s3_file in s3_files:
                local_path = write_file(raw_unload_path, s3_file)
                downloaded_files.append(local_path)

            # concatenate if requested
            if export_path:
                if export_path is True:
                    export_path = raw_unload_path
                concatenate_files(downloaded_files, export_path)

        # delete from S3 if requested
        if delete_s3_after:
            self.delete_from_s3(s3_path)

        return downloaded_files

    def insert_dataframe_to_table(
        self,
        dataframe,
        table_name,
        columns=None,
        create=False,
        metadata=None,
        batch_size=1000,
        verbose=False,
    ):
        """
        Insert a Pandas or Polars dataframe to an existing table or a new table.

        `executemany` in psycopg2 and pg8000 has very poor performance in terms of running speed.
        To overcome this issue, we instead format the insert query and then run `execute`.

        Parameters
        ----------
        dataframe: pandas.DataFrame or polars.DataFrame
            The pandas dataframe which needs to be inserted.

        table_name: str
            The name of the Snowflake table which is being inserted.

        columns: list, optional
            The list of columns which will be uploaded.

        create: bool, default False
            Boolean flag if a new table need to be created and insert to.

        metadata: dictionary, optional
            If metadata==None, it will be generated based on data

        batch_size: int, default 1000
            The number of records to insert in each batch

        verbose: bool, default False
            Whether or not to print out insert query


        """
        if columns:
            dataframe = dataframe[columns]

        all_columns = columns or list(dataframe.columns)
        column_sql = "(" + ",".join(all_columns) + ")"

        if not create and metadata:
            logger.warning("Metadata will not be used because create is set to False.")

        if create:
            if not metadata:
                logger.info("Metadata is missing. Generating metadata ...")
                metadata = find_column_type(dataframe, "redshift")
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
            create_query = f"CREATE TABLE {table_name} {create_join}"
            self.execute(create_query)
            logger.info("New table has been created")

        # create a list of tuples for insert
        @singledispatch
        def get_insert_tuple(dataframe, start, batch_size):
            """Create a list of tuples for insert."""
            pass

        @get_insert_tuple.register(pd.DataFrame)
        def get_insert_tuple_pandas(dataframe: pd.DataFrame, start, batch_size):
            """Create a list of tuples for insert when dataframe is pd.DataFrame."""
            to_insert = []
            for row in dataframe[start : (start + batch_size)].itertuples(index=False):
                none_row = (
                    "("
                    + ", ".join(
                        [
                            "NULL"
                            if pd.isnull(val)
                            else "'" + str(val).replace("'", "''") + "'"
                            for val in row
                        ]
                    )
                    + ")"
                )
                to_insert.append(none_row)
            return to_insert

        @get_insert_tuple.register(pl.DataFrame)
        def get_insert_tuple_polars(dataframe: pl.DataFrame, start, batch_size):
            """Create a list of tuples for insert when dataframe is pl.DataFrame."""
            to_insert = []
            dataframe = dataframe.with_columns(
                dataframe.select(cs.numeric().fill_nan(None))
            )
            for row in dataframe[start : (start + batch_size)].iter_rows():
                none_row = (
                    "("
                    + ", ".join(
                        [
                            "NULL"
                            if val is None
                            else "'" + str(val).replace("'", "''") + "'"
                            for val in row
                        ]
                    )
                    + ")"
                )
                to_insert.append(none_row)
            return to_insert

        logger.info("Inserting records...")
        try:
            for start in range(0, len(dataframe), batch_size):
                to_insert = get_insert_tuple(dataframe, start, batch_size)
                string_join = ", ".join(to_insert)
                insert_query = (
                    f"""INSERT INTO {table_name} {column_sql} VALUES {string_join}"""
                )
                self.execute(insert_query, verbose=verbose)
        except TypeError:
            raise TypeError(
                "DataFrame to insert must either be a pandas.DataFrame or polars.DataFrame."
            ) from None

        logger.info("Table insertion has completed")
