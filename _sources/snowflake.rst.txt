Snowflake Examples
==================

Upload to internal stage and run COPY into table command with YAML
------------------------------------------------------------------

.. code-block:: python

    import snowflake.connector
    import locopy

    create_sql = "CREATE TABLE namespace.table (variable VARCHAR(20))"


    with locopy.Snowflake(dbapi=snowflake.connector, config_yaml="example.yaml") as sf:
        sf.execute(create_sql)
        sf.upload_to_internal("example_data.csv", "@~/internal_stage")
        sf.copy("namespace.table", "@~/internal_stage/example_data.csv.gz")
        sf.execute("SELECT * FROM namespace.table")
        res = sf.cursor.fetchall()
    print(res)



Run COPY command without YAML
-----------------------------

Similar to the above code, but we would explicitly pass the connection details in the ``Snowflake``
constructor vs. the YAML

.. code-block:: python

    with locopy.Snowflake(
        dbapi=snowflake.connector,
        account='my.account.snowflake',
        warehouse="my-warehouse"
        database="db",
        user="userid",
        password="password",
        profile="aws_profile") as sf:
        ...



AWS tokens as environment variables
-----------------------------------

If you would rather provide your AWS tokens via environment variables vs. using a profile from
``~/.aws/credentials``, you can do something as follows:

.. code-block:: bash

    export AWS_ACCESS_KEY_ID=MY_ACCESS_KEY_ID
    export AWS_SECRET_ACCESS_KEY=MY_SECRET_ACCESS_KEY
    export AWS_SESSION_TOKEN=MY_SESSION_TOKEN



Extra parameters on the COPY job
--------------------------------

The  ``format_options`` and ``copy_options`` allow for adding additional parameters to the
``COPY into <table>`` commands. Both these options accept a list of strings. For example:

.. code-block:: python

    sf.copy(
      "namespace.table",
      "@~/internal_stage/example_data.csv.gz",
      file_type="csv",
      format_options=["FIELD_DELIMITER=','", "TRIM_SPACE = TRUE"],
      copy_options=["FORCE = TRUE", "PURGE = TRUE"])

These extra options will allow you to customize the COPY process to: trim spaces, force load, and
delete files after they have been successfully loaded.

To see a full list options check the `Snowflake documentation <https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#copy-into-table>`_
