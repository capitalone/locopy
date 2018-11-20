# MIT License
#
# Copyright (c) 2018 Capital One Services, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import filecmp
import pytest
import pg8000, psycopg2
import pandas as pd
import numpy as np
import boto3
import locopy

from pathlib import Path


DBAPIS = [pg8000, psycopg2]
INTEGRATION_CREDS = str(Path.home()) + "/.locopyrc"
S3_BUCKET = "locopy-integration-testing"
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_FILE = os.path.join(CURR_DIR, "data", "mock_file.txt")
LOCAL_FILE_DL = os.path.join(CURR_DIR, "data", "mock_file_dl.txt")

CREDS_DICT = locopy.utility.read_config_yaml(INTEGRATION_CREDS)


@pytest.fixture()
def s3_bucket():
    session = boto3.Session(profile_name=CREDS_DICT["profile"])
    c = session.client("s3")
    c.create_bucket(Bucket=S3_BUCKET)
    yield c
    r = session.resource("s3").Bucket(S3_BUCKET)
    r.objects.all().delete()
    r.delete()


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_redshift_execute_single_rows(dbapi):

    expected = pd.DataFrame({"field_1": [1], "field_2": [2]})
    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as test:
        test.execute("SELECT 1 AS field_1, 2 AS field_2 ")
        df = test.to_dataframe()

    assert np.allclose(df["field_1"], expected["field_1"])


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_redshift_execute_multiple_rows(dbapi):

    expected = pd.DataFrame({"field_1": [1, 2], "field_2": [1, 2]})
    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as test:
        test.execute(
            "SELECT 1 AS field_1, 1 AS field_2 " "UNION " "SELECT 2 AS field_1, 2 AS field_2"
        )
        df = test.to_dataframe()

    assert np.allclose(df["field_1"], expected["field_1"])
    assert np.allclose(df["field_2"], expected["field_2"])


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_s3_upload_download_file(s3_bucket, dbapi):

    s3 = locopy.S3(**CREDS_DICT)
    s3.upload_to_s3(LOCAL_FILE, S3_BUCKET, "myfile.txt")

    s3 = locopy.S3(**CREDS_DICT)
    s3.download_from_s3(S3_BUCKET, "myfile.txt", LOCAL_FILE_DL)

    assert filecmp.cmp(LOCAL_FILE, LOCAL_FILE_DL)
    os.remove(LOCAL_FILE_DL)


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_copy(s3_bucket, dbapi):

    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as redshift:
        redshift.execute(
            "CREATE TEMPORARY TABLE locopy_integration_testing (id INTEGER, variable VARCHAR(20)) DISTKEY(variable)"
        )
        redshift.run_copy(
            LOCAL_FILE,
            S3_BUCKET,
            "locopy_integration_testing",
            delim="|",
            delete_s3_after=True,
            compress=False,
        )
        redshift.execute("SELECT * FROM locopy_integration_testing ORDER BY id")
        results = redshift.cursor.fetchall()

        expected = [
            (1, "This iš line 1"),
            (2, "This is liné 2"),
            (3, "This is line 3"),
            (4, "This is lïne 4"),
        ]

        for i, result in enumerate(results):
            assert result[0] == expected[i][0]
            assert result[1] == expected[i][1]


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_unload(s3_bucket, dbapi):

    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as redshift:
        redshift.execute(
            "CREATE TEMPORARY TABLE locopy_integration_testing AS SELECT ('2017-12-31'::date + row_number() over (order by 1))::date from SVV_TABLES LIMIT 5"
        )
        sql = "SELECT * FROM locopy_integration_testing"
        redshift.run_unload(sql, S3_BUCKET, delimiter="|", export_path=LOCAL_FILE_DL)
        redshift.execute("SELECT * FROM locopy_integration_testing ORDER BY date")
        results = redshift.cursor.fetchall()

        expected = [
            ("2018-01-01",),
            ("2018-01-02",),
            ("2018-01-03",),
            ("2018-01-04",),
            ("2018-01-05",),
        ]

        for i, result in enumerate(results):
            assert result[0].strftime("%Y-%m-%d") == expected[i][0]

        os.remove(LOCAL_FILE_DL)
