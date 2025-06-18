"""PostgreSQL Module.

Module to wrap a database adapter into a PostgreSQL class which can be used to connect
to PostgreSQL, and run arbitrary code.
"""

from locopy.postgres_base import PostgresBase
from locopy.errors import DBError
from locopy.logger import INFO, get_logger

logger = get_logger(__name__, INFO)


class PostgreSQL(PostgresBase):
    """Locopy class which manages connections to PostgreSQL.

    If any of host, port, dbname, user and password are not provided, a config_yaml file must be
    provided with those parameters in it.

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
        Database credentials are not provided or valid
    """

    def __init__(self, dbapi=None, config_yaml=None, **kwargs):
        super().__init__(dbapi, config_yaml, **kwargs)

    def connect(self):
        """Create a connection to the PostgreSQL database.

        Sets the values of the ``conn`` and ``cursor`` attributes.

        Raises
        ------
        DBError
            If there is a problem establishing a connection to PostgreSQL.
        """
        if self.dbapi.__name__ == "psycopg2":
            self.connection["sslmode"] = "prefer"  # Default to prefer SSL but don't require it
        elif self.dbapi.__name__ == "pg8000":
            self.connection["ssl_context"] = True
        super().connect()

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
            raise DBError("No PostgreSQL connection object is present.")

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

        # Insert in batches
        total_rows = len(dataframe)
        for i in range(0, total_rows, batch_size):
            if verbose and i % (batch_size * 10) == 0:
                logger.info(f"Inserting rows {i} to {min(i + batch_size, total_rows)}")
            
            batch = dataframe.iloc[i:i + batch_size]
            values = [tuple(row) for row in batch[columns].values]
            self.execute(insert_sql, params=values, many=True, commit=True, verbose=verbose)

        if verbose:
            logger.info(f"Inserted {total_rows} rows into {table_name}") 