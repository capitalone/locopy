"""PostgreSQL Base Module.

Base module for PostgreSQL-like databases (PostgreSQL, Redshift, etc.) that provides common functionality.
"""

from functools import singledispatch
import pandas as pd
import polars as pl

from locopy.database import Database
from locopy.errors import DBError
from locopy.logger import INFO, get_logger
from locopy.utility import find_column_type

logger = get_logger(__name__, INFO)


class PostgresBase(Database):
    """Base class for PostgreSQL-like databases.

    Provides common functionality that can be used by both PostgreSQL and Redshift.

    Parameters
    ----------
    dbapi : DBAPI 2 module, optional
        A database adapter which is Python DB API 2.0 compliant
        (``psycopg2``, ``pg8000``, etc.)

    config_yaml : str, optional
        String representing the YAML file location of the database connection keyword arguments.

    **kwargs
        Database connection keyword arguments.
    """

    def __init__(self, dbapi=None, config_yaml=None, **kwargs):
        super().__init__(dbapi, config_yaml, **kwargs)

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
        """Insert a dataframe into a table.

        Parameters
        ----------
        dataframe : pandas.DataFrame or polars.DataFrame
            The dataframe to insert

        table_name : str
            The name of the table to insert into

        columns : list, optional
            List of column names. If not provided, will use the dataframe columns

        create : bool, optional
            Whether to create the table if it doesn't exist. Default False.

        metadata : dict, optional
            Column name to type mapping for table creation

        batch_size : int, optional
            How many rows to insert at once. Default 1000.

        verbose : bool, optional
            Whether to print progress. Default False.

        Raises
        ------
        DBError
            If there is an error executing the insert
        """
        if not self._is_connected():
            raise DBError("No database connection object is present.")

        # Use the dataframe columns if none provided
        if columns is None:
            columns = list(dataframe.columns)

        # Create table if requested
        if create:
            if metadata is None:
                # Infer types from dataframe if no metadata provided
                metadata = {col: find_column_type(dataframe[col]) for col in columns}
            
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            create_sql += ", ".join([f"{col} {metadata[col]}" for col in columns])
            create_sql += ")"
            self.execute(create_sql)

        # Build insert statement
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
        insert_sql += "(" + ", ".join(["%s"] * len(columns)) + ")"

        @singledispatch
        def get_insert_tuple(dataframe, start, batch_size):
            raise TypeError(f"Unsupported dataframe type: {type(dataframe)}")

        @get_insert_tuple.register(pd.DataFrame)
        def get_insert_tuple_pandas(dataframe: pd.DataFrame, start, batch_size):
            batch = dataframe.iloc[start:start + batch_size]
            return [tuple(row) for row in batch[columns].values]

        @get_insert_tuple.register(pl.DataFrame)
        def get_insert_tuple_polars(dataframe: pl.DataFrame, start, batch_size):
            batch = dataframe.slice(start, batch_size)
            return [tuple(row) for row in batch.select(columns).rows()]

        # Insert in batches
        total_rows = len(dataframe)
        for i in range(0, total_rows, batch_size):
            if verbose and i % (batch_size * 10) == 0:
                logger.info(f"Inserting rows {i} to {min(i + batch_size, total_rows)}")
            
            values = get_insert_tuple(dataframe, i, batch_size)
            self.execute(insert_sql, params=values, many=True, commit=True, verbose=verbose)

        if verbose:
            logger.info(f"Inserted {total_rows} rows into {table_name}")

    def _get_column_names(self, query):
        """Get column names from a query without executing it.

        Parameters
        ----------
        query : str
            SQL query to get column names from

        Returns
        -------
        list
            List of column names
        """
        try:
            # Add LIMIT 0 to avoid actually fetching data
            if "limit" not in query.lower():
                query = f"{query} LIMIT 0"
            self.execute(query)
            return self.column_names()
        except Exception as e:
            logger.error("Error getting column names. err: %s", e)
            raise DBError("Error getting column names.") from e 