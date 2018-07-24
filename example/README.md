# Usage Examples

- [Load Flat File with YAML](#yamlflat)
- [Load Flat File without YAML](#nonyamlflat)
- [Load Flat File with YAML and token environment variables](#tokensflat)
- [Extra parameters on the COPY job](#extraparams)


## <a name="yamlflat"></a>YAML: Upload to S3 and run COPY command
- no compression or splitting
- using `~/.aws/credentials` file

```python
import pg8000 # can also be psycopg2
import locopy

create_sql = """
              create table schema.table(
                id INTEGER,
                address VARCHAR(500),
                country VARCHAR(10),
                latitude DECIMAL(19,4),
                locality VARCHAR(500),
                longitude DECIMAL(19,4),
                name VARCHAR(200),
                postcode VARCHAR(10),
                region VARCHAR(10)
              )
              distkey(id)
              interleaved sortkey(id, postcode);
              """
with locopy.S3(
    dbapi=pg8000, config_yaml='example/example.yaml',
    profile='aws_profile') as s3:

    s3.execute(create_sql)
    s3.run_copy(
        local_file='example/example_data.csv',
        s3_bucket='my_s3_bucket',
        table_name='schema.table',
        delim=',',
        compress=False)
    s3.execute('SELECT * FROM schema.table')
    res = s3.cursor.fetchall()
print(res)
```


## <a name="nonyamlflat"></a>non-YAML: Upload to S3 and run COPY command

- no compression or splitting
- using `~/.aws/credentials` file

```python
import pg8000 # can also be psycopg2
import locopy

create_sql = """
              create table schema.table(
                id INTEGER,
                address VARCHAR(500),
                country VARCHAR(10),
                latitude DECIMAL(19,4),
                locality VARCHAR(500),
                longitude DECIMAL(19,4),
                name VARCHAR(200),
                postcode VARCHAR(10),
                region VARCHAR(10)
              )
              distkey(id)
              interleaved sortkey(id, postcode);
              """
with locopy.S3(
    dbapi=pg8000, host='my.redshift.cluster.com', port=5439, dbname='db',
    user='userid', password='password', profile='aws_profile') as s3:

    s3.execute(create_sql)
    s3.run_copy(
        local_file='example/example_data.csv',
        s3_bucket='my_s3_bucket',
        table_name='schema.table',
        delim=',')
    s3.execute('SELECT * FROM schema.table')
    res = s3.cursor.fetchall()
print(res)
```

## <a name="tokensflat"></a>YAML and token environment variables

- no compression or splitting
- using AWS environment variables

```bash
export AWS_ACCESS_KEY_ID=MY_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=MY_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN=MY_SESSION_TOKEN
```


```python
import pg8000 # can also be psycopg2
import locopy

create_sql = """
              create table schema.table(
                id INTEGER,
                address VARCHAR(500),
                country VARCHAR(10),
                latitude DECIMAL(19,4),
                locality VARCHAR(500),
                longitude DECIMAL(19,4),
                name VARCHAR(200),
                postcode VARCHAR(10),
                region VARCHAR(10)
              )
              distkey(id)
              interleaved sortkey(id, postcode);
              """
with locopy.S3(dbapi=pg8000, config_yaml='example/example.yaml') as s3:
    s3.execute(create_sql)
    s3.run_copy(
        local_file='example/example_data.csv',
        s3_bucket='my_s3_bucket',
        table_name='schema.table',
        delim=',',
        compress=False)
    s3.execute('SELECT * FROM schema.table')
    res = s3.cursor.fetchall()
print(res)
```

## <a name="extraparams"></a>Extra parameters on the COPY job
As per the AWS documentation [here](http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html),
there may be times when you want to tweak the options used by the COPY job if
you have some oddities in your data.  Locopy by assigns a few options by default
(`DATEFORMAT 'auto'`, `COMPUPDATE ON`, and `TRUNCATECOLUMNS`).  If you want to
specify other options, or override these three, you can pass in a list of
strings which will tweak your load:

```python
# (Same init as above)
s3.run_copy("example/example_data.csv",
            "my_s3_bucket",
            "schema.table",
            delim=",",
            ["NULL AS 'NULL'", "ESCAPE"])
```
