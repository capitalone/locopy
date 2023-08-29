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
from pathlib import Path

from .database import Database
from .errors import DBError, S3CredentialsError
from .logger import INFO, get_logger
from .s3 import S3
from .utility import (compress_file_list, concatenate_files, find_column_type,
                      get_ignoreheader_number, split_file, write_file)

logger = get_logger(__name__, INFO)


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
    """Returns the ``copy_options`` attribute with spaces in between and as
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

    def __init__(
        self, profile=None, kms_key=None, dbapi=None, config_yaml=None, **kwargs
    ):
        try:
            S3.__init__(self, profile, kms_key)
        except S3CredentialsError:
            logger.warning(
                "S3 credentials were not found. S3 functionality is disabled"
            )
        Database.__init__(self, dbapi, config_yaml, **kwargs)

    def connect(self):
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
            self.connection["ssl_context"] = True
        super(Redshift, self).connect()

    def copy(self, table_name, s3path, delim="|", copy_options=None):
        """Executes the COPY command to load files from S3 into
        a Redshift table.

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
        if copy_options and "PARQUET" not in copy_options or copy_options is None:
            copy_options = add_default_copy_options(copy_options)
        if delim:
            copy_options = [f"DELIMITER '{delim}'"] + copy_options
        copy_options_text = combine_copy_options(copy_options)
        base_copy_string = "COPY {0} FROM '{1}' " "CREDENTIALS '{2}' " "{3};"
        try:
            sql = base_copy_string.format(
                table_name, s3path, self._credentials_string(), copy_options_text
            )
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error("Error running COPY on Redshift. err: %s", e)
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
            The local file which you wish to copy. This can be a folder for non-delimited file type like parquet

        s3_bucket : str
            The AWS S3 bucket which you are copying the local file to.

        table_name : str
            The Redshift table name which is being loaded

        delim : str, optional
            Delimiter for Redshift ``COPY`` command. None for non-delimited files. Defaults to ``|``.

        copy_options : list, optional
            A list (str) of copy options that should be appended to the COPY
            statement.  The class will insert a default for DATEFORMAT,
            COMPUPDATE and TRUNCATECOLUMNS if they are not provided in this
            list if PARQUET is not part of the options passed in
            See http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html
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
        # We need to check if IGNOREHEADER is set as this can cause issues.
        ignore_header = get_ignoreheader_number(copy_options)
        p = Path(local_file)
        if p.is_dir():
            upload_list = [str(x) for x in p.glob("**/*") if x.is_file()]
        else:
            upload_list = split_file(
                local_file, local_file, splits=splits, ignore_header=ignore_header
            )

        if splits > 1 and ignore_header > 0:
            # remove the IGNOREHEADER from copy_options
            logger.info("Removing the IGNOREHEADER option as split is enabled")
            copy_options = [
                i for i in copy_options if not i.startswith("IGNOREHEADER ")
            ]

        if compress:
            copy_options.append("GZIP")
            upload_list = compress_file_list(upload_list)

        # copy files to S3
        s3_upload_list = self.upload_list_to_s3(upload_list, s3_bucket, s3_folder)
        if p.is_dir():
            if s3_folder:
                bucket = Path(s3_bucket)
                folder = Path(s3_folder)
                tmp_load_path = str(bucket / folder)
            else:
                tmp_load_path = s3_bucket
        else:
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
        raw_unload_path=None,
        export_path=False,
        delim=",",
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

        raw_unload_path : str, optional
            The local path where the files will be copied to. Defaults to the current working
            directory (``os.getcwd()``).

        export_path : str, optional
            If a ``export_path`` is provided, function will concatenate and write the unloaded
            files to this path as a single file. If your file is very large you may not want to
            use this option.

        delim : str, optional
            Delimiter for unloading and file writing. Defaults to a comma. If None, this option will be ignored

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
        if delim:
            unload_options.append("DELIMITER '{0}'".format(delim))
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
        local_list = self.download_list_from_s3(s3_download_list, raw_unload_path)
        if export_path:
            write_file([columns], delim, export_path)  # column
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
        base_unload_string = (
            "UNLOAD ('{0}')\n" "TO '{1}'\n" "CREDENTIALS '{2}'\n" "{3};"
        )

        try:
            sql = base_unload_string.format(
                query.replace("'", r"\'"),
                s3path,
                self._credentials_string(),
                unload_options_text,
            )
            self.execute(sql, commit=True)
        except Exception as e:
            logger.error("Error running UNLOAD on redshift. err: %s", e)
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
        sql = (
            "SELECT path FROM stl_unload_log "
            "WHERE query = pg_last_query_id() ORDER BY path"
        )
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
        Insert a Pandas dataframe to an existing table or a new table.

        `executemany` in psycopg2 and pg8000 has very poor performance in terms of running speed.
        To overcome this issue, we instead format the insert query and then run `execute`.

        Parameters
        ----------
        dataframe: Pandas Dataframe
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

        import pandas as pd

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
            create_query = "CREATE TABLE {table_name} {create_join}".format(
                table_name=table_name, create_join=create_join
            )
            self.execute(create_query)
            logger.info("New table has been created")

        logger.info("Inserting records...")
        for start in range(0, len(dataframe), batch_size):
            # create a list of tuples for insert
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
            string_join = ", ".join(to_insert)
            insert_query = (
                """INSERT INTO {table_name} {columns} VALUES {values}""".format(
                    table_name=table_name, columns=column_sql, values=string_join
                )
            )
            self.execute(insert_query, verbose=verbose)
        logger.info("Table insertion has completed")
