.. image:: https://travis-ci.org/capitalone/Data-Load-and-Copy-using-Python.svg?branch=master
    :target: https://travis-ci.org/capitalone/Data-Load-and-Copy-using-Python
.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black

locopy: Data Load and Copy using Python
========================================

A Python library to assist with ETL processing for:

- Amazon Redshift (``COPY``, ``UNLOAD``)
- Snowflake (``COPY INTO <table>``, ``COPY INTO <location>``)

In addition:

- The library supports Python 3.5+
- DB Driver (Adapter) agnostic. Use your favourite driver that complies with
  `DB-API 2.0 <https://www.python.org/dev/peps/pep-0249/>`_
- It provides functionality to download and upload data to S3 buckets, and internal stages (Snowflake)


Quick Installation
==================

.. code-block:: bash

    pip install locopy


Installation instructions
-------------------------

A virtual environment is highly recommended

.. code-block:: bash

    $ virtualenv locopy
    $ source locopy/bin/activate
    $ pip install --upgrade setuptools pip
    $ pip install locopy


Python Database API Specification 2.0
-------------------------------------

Rather than using a specific Python DB Driver / Adapter for Postgres (which should supports Amazon
Redshift or Snowflake), ``locopy`` prefers to be agnostic. As an end user you can use any Python
Database API Specification 2.0 package.

The following packages have been tested:

- ``psycopg2``
- ``pg8000``
- ``snowflake-connector-python``

You can use which ever one you prefer by importing the package and passing it
into the constructor input ``dbapi``.



Usage
-----

You need to store your connection parameters in a YAML file (or pass them in directly).
The YAML would consist of the following items:

.. code-block:: yaml

    # required to connect to redshift
    host: my.redshift.cluster.com
    port: 5439
    database: db
    user: userid
    password: password
    ## optional extras for the dbapi connector
    sslmode: require
    another_option: 123



If you aren't loading data, you don't need to have AWS tokens set up.
The Redshift connection (``Redshift``) can be used like this:

.. code-block:: python

    import pg8000
    import locopy

    with locopy.Redshift(dbapi=pg8000, config_yaml="config.yml") as redshift:
        redshift.execute("SELECT * FROM schema.table")
        df = redshift.to_dataframe()
    print(df)


If you want to load data to Redshift via S3, the ``Redshift`` class inherits from ``S3``:

.. code-block:: python

    import pg8000
    import locopy

    with locopy.Redshift(dbapi=pg8000, config_yaml="config.yml") as redshift:
        redshift.execute("SET query_group TO quick")
        redshift.execute("CREATE TABLE schema.table (variable VARCHAR(20)) DISTKEY(variable)")
        redshift.load_and_copy(
            local_file="example/example_data.csv",
            s3_bucket="my_s3_bucket",
            table_name="schema.table",
            delim=",")
        redshift.execute("SELECT * FROM schema.table")
        res = redshift.cursor.fetchall()

    print(res)


If you want to download data from Redshift to a CSV, or read it into Python

.. code-block:: python

    my_profile = "some_profile_with_valid_tokens"
    with locopy.Redshift(dbapi=pg8000, config_yaml="config.yml", profile=my_profile) as redshift:
        ##Optionally provide export if you ALSO want the exported data copied to a flat file
        redshift.unload_and_copy(
            query="SELECT * FROM schema.table",
            s3_bucket="my_s3_bucket",
            export_path="my_output_destination.csv")



Note on tokens
^^^^^^^^^^^^^^

To load data to S3, you will need to be able to generate AWS tokens, or assume the IAM role on a EC2
instance. There are a few options for doing this, depending on where you're running your script and
how you want to handle tokens. Once you have your tokens, they need to be accessible to the AWS
command line interface. See
http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#config-settings-and-precedence
for more information, but you can:

- Populate environment variables ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
  etc.
- Leverage the AWS credentials file.  If you have multiple profiles configured
  you can either call ``locopy.Redshift(profile="my-profile")``, or set up an
  environment variable ``AWS_DEFAULT_PROFILE``.
- If you are on a EC2 instance you can assume the credentials associated with the IAM role attached.


Advanced Usage
--------------

See the `docs <https://capitalone.github.io/Data-Load-and-Copy-using-Python/>`_ for
more detailed usage instructions and examples including Snowflake.


Contributors
------------

We welcome your interest in Capital One’s Open Source Projects (the "Project").
Any Contributor to the project must accept and sign a CLA indicating agreement to
the license terms. Except for the license granted in this CLA to Capital One and
to recipients of software distributed by Capital One, you reserve all right, title,
and interest in and to your contributions; this CLA does not impact your rights to
use your own contributions for any other purpose.

- `Link to Individual CLA <https://docs.google.com/forms/d/19LpBBjykHPox18vrZvBbZUcK6gQTj7qv1O5hCduAZFU/viewform>`_
- `Link to Corporate CLA <https://docs.google.com/forms/d/e/1FAIpQLSeAbobIPLCVZD_ccgtMWBDAcN68oqbAJBQyDTSAQ1AkYuCp_g/viewform>`_

This project adheres to the `Open Source Code of Conduct <https://developer.capitalone.com/resources/code-of-conduct>`_.
By participating, you are expected to honor this code.
