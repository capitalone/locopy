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

"""Database Module
"""
import time

from .logger import get_logger, DEBUG, INFO, WARN, ERROR, CRITICAL
from .errors import CredentialsError, DBError
from .utility import read_config_yaml

logger = get_logger(__name__, INFO)


class Database(object):
    """This is the base class for all DBAPI 2 database connectors which will inherit this
    functionality. The ``Database`` class will manage connections and handle executing queries.
    Most of the functionality should work out of the box for classes which inherit minus the
    abstract method for ``_connect`` which may vary across databases.

    Parameters
    ----------
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
        Database credentials are not provided, valid, or both kwargs and a YAML config was provided.
    """

    def __init__(self, dbapi, config_yaml=None, **kwargs):
        self.dbapi = dbapi
        self.connection = kwargs or {}
        self.conn = None
        self.cursor = None

        if config_yaml and self.connection:
            raise CredentialsError("Please provide kwargs or a YAML configuraton, not both.")
        if config_yaml:
            self.connection = read_config_yaml(config_yaml)

    def _connect(self):
        """Creates a connection to a database by setting the values of the ``conn`` and ``cursor``
        attributes.

        Raises
        ------
        DBError
            If there is a problem establishing a connection.
        """
        try:
            self.conn = self.dbapi.connect(**self.connection)
            self.cursor = self.conn.cursor()
        except Exception as e:
            logger.error("Error connecting to the database. err: %s", e)
            raise DBError("Error connecting to the database.")

    def _disconnect(self):
        """Terminates the connection by closing the values of the ``conn`` and ``cursor``
        attributes.

        Raises
        ------
        DBError
            If there is a problem disconnecting from the database.
        """
        if self._is_connected():
            try:
                # close connections
                self.cursor.close()
                self.conn.close()
            except Exception as e:
                logger.error("Error disconnecting from the database. err: %s", e)
                raise DBError("There is a problem disconnecting from the database.")
        else:
            logger.info("No connection to close")

    def execute(self, sql, commit=True, params=None):
        """Execute some sql against the connection.

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
        DBError
            if a problem occurs executing the ``sql`` statement

        DBError
            If a connection to the database cannot be made
        """
        if self._is_connected():
            start_time = time.time()
            logger.info("Running SQL: %s", sql)
            try:
                self.cursor.execute(sql, params)
            except Exception as e:
                logger.error("Error running SQL query. err: %s", e)
                raise DBError("Error running SQL query.")
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
            raise DBError("Cannot execute SQL on a closed connection.")

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
        logger.info("Connecting...")
        self._connect()
        logger.info("Connection established.")
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        logger.info("Closing connection...")
        self._disconnect()
        logger.info("Connection closed.")
