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
from .utility import ProgressPercentage, compress_file_list, split_file, write_file
from .logger import get_logger, DEBUG, INFO, WARN, ERROR, CRITICAL
from .errors import CredentialsError, DBError, S3CredentialsError

logger = get_logger(__name__, INFO)


def combine_options(options=None):
    """ Returns the ``copy_options`` or ``format_options`` attribute with spaces in between and as
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

    def __init__(self, profile=None, kms_key=None, dbapi=None, config_yaml=None, **kwargs):
        try:
            S3.__init__(self, profile, kms_key)
        except S3CredentialsError:
            logger.warn("S3 credentials we not found. S3 functionality is disabled")
            logger.warn("Only internal stages are available")
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
            self.execute("USE WAREHOUSE {0}".format(self.connection["warehouse"]))
        if self.connection.get("database") is not None:
            self.execute("USE DATABASE {0}".format(self.connection["database"]))

    def upload_to_internal(self, local, stage, parallel=4, auto_compress=True):
        """
        Upload file(s) to a internal stage via the ``PUT`` command.

        Parameters
        ----------
        local : str
            The local directory path to the file to upload. Wildcard characters (``*``, ``?``) are
            supported to enable uploading multiple files in a directory.

        stage : str
            Internal stage location to load the file.

        parallel : int, optional
            Specifies the number of threads to use for uploading files.

        auto_compress : bool, optional
            Specifies if Snowflake uses gzip to compress files during upload.
            If ``True``, the files are compressed (if they are not already compressed).
            if ``False``, the files are uploaded as-is.
        """
        self.execute(
            "PUT file://{0} {1} PARALLEL={2} AUTO_COMPRESS={3}".format(
                local, stage, parallel, auto_compress
            )
        )

    def download_from_internal(self, stage, local=os.getcwd(), parallel=10):
        """
        Download file(s) from a internal stage via the ``GET`` command.

        Parameters
        ----------
        stage : str
            Internal stage location to load the file. Can include folders so that

        local : str, optional
            The local directory path where files will be downloaded to. Defualts to the current
            working directory (``os.getcwd()``)

        parallel : int, optional
            Specifies the number of threads to use for downloading files.
        """
        self.execute("GET {0} file://{1} PARALLEL={2}".format(stage, local, parallel))

    def copy(
        self, table_name, stage, delim="|", header=False, format_options=None, copy_options=None
    ):
        """Executes the ``COPY INTO <table>`` command to load CSV files from a stage into
        a Snowflake table.

        Parameters
        ----------
        table_name : str
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
        DBError
            If there is a problem executing the COPY command, or a connection
            has not been initalized.
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
                table_name, stage, delim, int(header), format_options_text, copy_options_text
            )
            self.execute(sql, commit=True)

        except Exception as e:
            logger.error("Error running COPY on Snowflake. err: %s", e)
            raise DBError("Error running COPY on Snowflake.")

    def unload(
        self, stage, table_name, delim="|", header=False, format_options=None, copy_options=None
    ):
        """Executes the ``COPY INTO <location>`` command to export a query/table from
        Snowflake to a stage.

        Parameters
        ----------
        stage : str
            Stage location (internal or external) where the data files are unloaded

        table_name : str
            The Snowflake table name which is being unloaded. Must be fully qualified:
            ``<namespace>.<table_name>``

        delim : str
            The delimiter in a delimited file.

        header : bool, optional
            Boolean flag if header is included in the file(s)

        format_options : list
            List of strings of format options to provide to the ``COPY INTO`` command.

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

        format_options_text = combine_options(format_options)
        copy_options_text = combine_options(copy_options)
        base_unload_string = (
            "COPY INTO {0} FROM {1} "
            "FILE_FORMAT = (TYPE='csv' FIELD_DELIMITER='{2}' {3}) HEADER={4} {5}"
        )

        try:
            sql = base_unload_string.format(
                stage, table_name, delim, format_options_text, header, copy_options_text
            )
            self.execute(sql, commit=True)
        except Exception as e:
            logger.error("Error running UNLOAD on Snowflake. err: %s", e)
            raise DBError("Error running UNLOAD on Snowflake.")
