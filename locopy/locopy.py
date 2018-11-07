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
Module to wrap a database adapter into a Cmd class which can be used to connect
to Redshift, and run arbitrary code.
"""

import time

from .utility import validate_redshift_attributes, get_redshift_yaml
from .logger import get_logger, DEBUG, INFO, WARN, ERROR, CRITICAL
from .errors import (
    RedshiftCredentialsError,
    RedshiftConnectionError,
    RedshiftDisconnectionError,
    RedshiftError,
)

logger = get_logger(__name__, INFO)


class Cmd(object):
    """
    Locopy class which manages connections to Redshift.  Subclassed as S3
    elsewhere to provide the COPY functionality.

    If any of host, port, dbname, user and password are not provided, a
    config_yaml file must be provided with those parameters in.

    Parameters
    ----------
    dbapi : DBAPI 2 module
        A PostgreSQL database adapter which is Python DB API 2.0 compliant
        (``psycopg2``, ``pg8000``, etc.)

    host : str
        Host name of the Redshift cluster to connect to.

    port : int
        Port which connection will be made to Redshift.

    dbname : str
        Redshift database name.

    user : str
        Redshift users username.

    password : str
        Redshift users password.

    config_yaml : str
        String representing the file location of the credentials.

    Raises
    ------
    RedshiftCredentialsError
        Redshift credentials are not provided
    """

    def __init__(
        self,
        dbapi=None,
        host=None,
        port=None,
        dbname=None,
        user=None,
        password=None,
        config_yaml=None,
        **kwargs
    ):

        self.dbapi = dbapi
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None

        try:
            validate_redshift_attributes(host, port, dbname, user, password)
        except:
            try:
                atts = get_redshift_yaml(config_yaml)
                self.host = atts["host"]
                self.port = atts["port"]
                self.dbname = atts["dbname"]
                self.user = atts["user"]
                self.password = atts["password"]
            except Exception as e:
                logger.error("Must provide Redshift attributes or YAML. err: %s", e)
                raise RedshiftCredentialsError("Must provide Redshift attributes or YAML.")
        self._connect()

    def _connect(self):
        """Creates a connection to the Redshift cluster by
        setting the values of the ``conn`` and ``cursor`` attributes.

        Raises
        ------
        RedshiftConnectionError
            If there is a problem establishing a connection to Redshift.
        """
        extra = {}
        if self.dbapi.__name__ == "psycopg2":
            extra = {"sslmode": "require"}
        elif self.dbapi.__name__ == "pg8000":
            extra = {"ssl": True}

        try:
            self.conn = self.dbapi.connect(
                host=self.host,
                user=self.user,
                port=self.port,
                password=self.password,
                database=self.dbname,
                **extra
            )

            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error("Error connecting to Redshift. err: %s", e)
            raise RedshiftConnectionError("There is a problem connecting to Redshift.")

    def _disconnect(self):
        """Terminates the connection to the Redshift cluster by
        closing the values of the ``conn`` and ``cursor`` attributes.

        Raises
        ------
        RedshiftDisconnectionError
            If there is a problem disconnecting from Redshift.
        """
        if self._is_connected():
            try:
                # close connections
                self.cursor.close()
                self.conn.close()
            except Exception as e:
                logger.error("Error disconnecting from Redshift. err: %s", e)
                raise RedshiftDisconnectionError("There is a problem disconnecting from Redshift.")
        else:
            logger.info("No connection to close")

    def execute(self, sql, commit=True, params=None):
        """Execute some sql against the Redshift connection.

        Parameters
        ----------
        sql : str
            SQL to run against the connection.  Could be one or multiple
            statements.

        commit : Boolean, default True
            Whether to "commit" the commands to the cluster immediately or not.

        params : iterable of parameters
            Parameters to submit with the query. The exact syntax will depend
            on the database adapter you are using

        Raises
        ------
        RedshiftError
            if a problem occurs executing the ``sql`` statement

        RedshiftConnectionError
            If a connection to Redshift cannot be made
        """
        if self._is_connected():
            start_time = time.time()
            logger.info("Running SQL: %s", sql)
            try:
                self.cursor.execute(sql, params)
            except Exception as e:
                logger.error("Error running SQL query. err: %s", e)
                raise RedshiftError("Error running SQL query.")
            if commit:
                self.conn.commit()
            elapsed = time.time() - start_time
            if elapsed >= 60:
                time_str = str(int(elapsed / 60)) + " minutes, "
            else:
                time_str = ""
            time_str += str(int(elapsed) % 60) + " seconds"
            logger.info("Time elapsed: %s", time_str)
        else:
            raise RedshiftConnectionError("Cannot execute SQL on a closed connection.")

    def column_names(self):
        """Pull column names out of the cursor description. Depending on the
        DBAPI, it could return column names as bytes: ``b'column_name'``

        Returns
        -------
        list
            List of column names, all in lower-case
        """
        try:
            return [column[0].decode().lower() for column in self.cursor.description]
        except:
            return [column[0].lower() for column in self.cursor.description]

    def to_dataframe(self, size=None):
        """Return a dataframe of the last query results.  This imports Pandas
        in here, so that it's not needed for other use cases.  This is just a
        convenience method.

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
        import pandas

        columns = self.column_names()

        if size is None:
            fetched = self.cursor.fetchall()
        else:
            fetched = self.cursor.fetchmany(size)

        # pg8000 returns a tuple of lists vs list of tuples.
        # https://github.com/mfenniak/pg8000/issues/163
        fetched = [tuple(column for column in row) for row in fetched]

        if len(fetched) == 0:
            return None
        return pandas.DataFrame(fetched, columns=columns)

    def _is_connected(self):
        """Checks the connection and cursor class arrtribues are initalized.

        Returns
        -------
        bool
            True if conn and cursor are not ``None``, False otherwise.
        """
        try:
            return self.conn is not None and self.cursor is not None
        except:
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        logger.info("Closing Redshift connection...")
        self._disconnect()
        logger.info("Connection closed.")
