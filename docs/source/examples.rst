Basic Examples
==============

Upload to S3 and run COPY command with YAML
-------------------------------------------

- no compression or splitting
- using ``~/.aws/credentials`` file

.. code-block:: python

    import pg8000 # can also be psycopg2
    import locopy

    create_sql = "CREATE TABLE schema.table (variable VARCHAR(20)) DISTKEY(variable)"

    with locopy.S3(
        dbapi=pg8000,
        config_yaml="example.yaml",
        profile="aws_profile") as s3:

        s3.execute(create_sql)
        s3.run_copy(
            local_file="example_data.csv",
            s3_bucket="my_s3_bucket",
            table_name="schema.table",
            delim=",",
            compress=False)
        s3.execute("SELECT * FROM schema.table")
        res = s3.cursor.fetchall()
    print(res)



Upload to S3 and run COPY command without YAML
----------------------------------------------

Identical to the above code, but we would explicitly pass the connection details in the ``S3``
constructor vs. the YAML

.. code-block:: python

    with locopy.S3(
        dbapi=pg8000,
        host="my.redshift.cluster.com",
        port=5439,
        dbname="db",
        user="userid",
        password="password",
        profile="aws_profile") as s3:
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

As per the AWS documentation `here <http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html>`_,
there may be times when you want to tweak the options used by the ``COPY`` job if you have some
oddities in your data.  Locopy assigns a few options by default (``DATEFORMAT 'auto'``,
``COMPUPDATE ON``, and ``TRUNCATECOLUMNS``). If you want to specify other options, or override these
three, you can pass in a list of strings which will tweak your load:

.. code-block:: python

    s3.run_copy(local_file="example_data.csv",
                s3_bucket="my_s3_bucket",
                "schema.table",
                delim=",",
                copy_options=["NULL AS 'NULL'", "ESCAPE"])
