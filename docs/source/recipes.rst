Common Recipes
==============

Lets go over some common use cases people will have when interacting with
``locopy``.


I have a file I want to load to Redshift
----------------------------------------
This is probably the most common use case. What you need to start:

- Read/Write S3 access
- Read/Write Redshift Access

There are a couple of ways you can authenticate with Redshift. If you are running
this process on a regular basis it might make sense to use a ``YAML`` file to
store (in a secure place) most of the details. You will also need valid AWS
tokens in order to access the appropriate AWS resources (S3 bucket).

Assuming you have your ``YAML`` file setup, the following will occur:

.. code-block:: python

    import pg8000
    import locopy

    with locopy.S3(dbapi=pg8000, config_yaml="example.yaml") as s3:
        s3.run_copy(
            local_file="some_data_to_load.txt",
            s3_bucket="s3_bucket_to_use",
            s3_folder="s3_folder_to_use/s3_subfolder_to_use",
            table_name="redshift_table_to_load",
            delim=",")

- ``some_data_to_load.txt`` will be compressed (gzip) and the original file deleted
- ``some_data_to_load.txt`` will be uploaded to the S3 bucket:
  ``s3_bucket_to_use`` at the location of ``s3_folder_to_use/s3_subfolder_to_use``
- A ``COPY`` command will be executed to load the contents of
  ``some_data_to_load.txt`` to ``redshift_table_to_load`` using the delimiter ``,``


I'd like to split my file into *n* files
----------------------------------------
You can tweak the above code to include the ``splits`` option which takes an
integer

.. code-block:: python

    import pg8000
    import locopy

    with locopy.S3(dbapi=pg8000, config_yaml="example.yaml") as s3:
        s3.run_copy(
            local_file="some_data_to_load.txt",
            s3_bucket="s3_bucket_to_use",
            s3_folder="s3_folder_to_use/s3_subfolder_to_use",
            table_name="redshift_table_to_load",
            delim=",",
            splits=10)

``splits`` is really useful when you have a **very large** file you want to load.
Ideally this number will equal the number of slices available to you on the
Redshift cluster.


I don't want to compress my files
---------------------------------
By default compression is always on. If you'd like to turn if off you can set
``compress=False``

.. code-block:: python

    import pg8000
    import locopy

    with locopy.S3(dbapi=pg8000, config_yaml="example.yaml") as s3:
        s3.run_copy(
            local_file="some_data_to_load.txt",
            s3_bucket="s3_bucket_to_use",
            s3_folder="s3_folder_to_use/s3_subfolder_to_use",
            table_name="redshift_table_to_load",
            delim=",",
            compress=False)

Depending on your own requirements it might make sense to turn off compression
or leave it on. Combining this with ``splits`` can drastically alter the
performance of the ``COPY`` command


I want to export some data from Redshift to a local CSV
-------------------------------------------------------
Data can be exported from Redshift to a CSV by supplying an ``export_path``
to locopy.S3.run_unload():

.. code-block:: python

    import pg8000
    import locopy

    my_profile = "some_profile_with_valid_tokens"
    with locopy.S3(dbapi=pg8000, config_yaml="config.yml", profile=my_profile) as s3:
        s3.run_unload(
            query="SELECT * FROM schema.table",
            s3_bucket="s3_bucket_to_use",
            export_path="output.csv")

Or a pipe delimited....

.. code-block:: python

    with locopy.S3(config_yaml="config.yml", profile=my_profile) as s3:
        s3.run_unload(
            query="SELECT * FROM schema.table",
            s3_bucket="s3_bucket_to_use",
            export_path="output.tsv",
            delimiter="|")

.. note::
  If your bucket has previously unloaded files, you may get an
  error when unloading. If you don't want to delete the older files, you can
  unload your data to a new folder or with a different prefix by specifying the
  ``s3_folder`` parameter. You can specify a folder to write to, i.e.
  ``s3_folder=s3_folder_to_use/``, or create a unique filename prefix name by
  omitting the last ``/``, i.e. ``s3_folder=unique_file_prefix``.

.. code-block:: python

    with locopy.S3(dbapi=pg8000, config_yaml="config.yml", profile=my_profile) as s3:
        s3.run_unload(
            query="SELECT * FROM schema.table",
            s3_bucket="s3_bucket_to_use",
            s3_folder="s3_folder_to_use/",
            export_path="output.csv")


I want to backup my table to S3
------------------------------------
To simply export data to S3 and do nothing else, omit the ``export_path`` option
so that the file is not downloaded, and set ``delete_s3_after=False`` to prevent
the S3 files from being automatically deleted after the run.

.. code-block:: python

    import pg8000
    import locopy

    with locopy.S3(dbapi=pg8000, config_yaml="config.yml", profile=my_profile) as s3:
        s3.run_unload(
            query="SELECT * FROM schema.table",
            s3_bucket="s3_bucket_to_use",
            s3_folder="s3_folder_to_use/s3_subfolder_to_use/",
            delete_s3_after=False) # defaults to True

By default, the Redshift unloads data to multiple files in S3 for performance
reasons. The maximum size for a data file is 6.2 GB. If the data size is
greater than the maximum, UNLOAD creates additional files, up to 6.2 GB each.
If you want to back it up as a single file, you can run:

.. code-block:: python

    with locopy.S3(dbapi=pg8000, config_yaml="config.yml", profile=my_profile) as s3:
        s3.run_unload(
            query="SELECT * FROM schema.table",
            s3_bucket="s3_bucket_to_use",
            s3_folder="s3_folder_to_use/s3_subfolder_to_use/",
            delete_s3_after=False,
            parallel_off=True)
