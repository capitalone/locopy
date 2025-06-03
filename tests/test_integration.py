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

import filecmp
import os
from pathlib import Path

import boto3
import locopy
import numpy as np
import pandas as pd
import pg8000
import psycopg2
import pytest

DBAPIS = [pg8000, psycopg2]
INTEGRATION_CREDS = str(Path.home()) + "/.locopyrc"
S3_BUCKET = "locopy-integration-testing"
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_FILE = os.path.join(CURR_DIR, "data", "mock_file.txt")
LOCAL_FILE_HEADER = os.path.join(CURR_DIR, "data", "mock_file_header.txt")
LOCAL_FILE_DL = os.path.join(CURR_DIR, "data", "mock_file_dl.txt")
UNLOAD_PATH = os.path.join(CURR_DIR, "data", "unload_path")
TEST_DF = pd.read_csv(os.path.join(CURR_DIR, "data", "mock_dataframe.txt"), sep=",")
TEST_DF_2 = pd.read_csv(os.path.join(CURR_DIR, "data", "mock_dataframe_2.txt"), sep=",")

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
            "SELECT 1 AS field_1, 1 AS field_2 UNION SELECT 2 AS field_1, 2 AS field_2"
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
        redshift.load_and_copy(
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
def test_copy_split_ignore(s3_bucket, dbapi):
    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as redshift:
        redshift.execute(
            "CREATE TEMPORARY TABLE locopy_integration_testing (id INTEGER, variable VARCHAR(20)) DISTKEY(variable)"
        )
        redshift.load_and_copy(
            LOCAL_FILE_HEADER,
            S3_BUCKET,
            "locopy_integration_testing",
            delim="|",
            delete_s3_after=True,
            compress=False,
            splits=4,
            copy_options=["IGNOREHEADER as 1"],
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
            os.remove(LOCAL_FILE_HEADER + f".{i}")


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_unload(s3_bucket, dbapi):
    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as redshift:
        redshift.execute(
            "CREATE TEMPORARY TABLE locopy_integration_testing AS SELECT ('2017-12-31'::date + row_number() over (order by 1))::date from SVV_TABLES LIMIT 5"
        )
        sql = "SELECT * FROM locopy_integration_testing"
        redshift.unload_and_copy(sql, S3_BUCKET, delim="|", export_path=LOCAL_FILE_DL)
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


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_unload_raw_unload_path(s3_bucket, dbapi):
    os.mkdir(UNLOAD_PATH)

    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as redshift:
        redshift.execute(
            "CREATE TEMPORARY TABLE locopy_integration_testing AS SELECT ('2017-12-31'::date + row_number() over (order by 1))::date from SVV_TABLES LIMIT 5"
        )
        sql = "SELECT * FROM locopy_integration_testing"
        redshift.unload_and_copy(
            sql,
            S3_BUCKET,
            delim="|",
            raw_unload_path=UNLOAD_PATH,
            export_path=LOCAL_FILE_DL,
        )
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

        os.rmdir(UNLOAD_PATH)
        os.remove(LOCAL_FILE_DL)


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_insert_dataframe_to_table(s3_bucket, dbapi):
    with locopy.Redshift(dbapi=dbapi, **CREDS_DICT) as redshift:
        redshift.insert_dataframe_to_table(TEST_DF, "locopy_df_test", create=True)
        redshift.execute("SELECT a, b, c FROM locopy_df_test ORDER BY a ASC")
        results = redshift.cursor.fetchall()
        redshift.execute("drop table if exists locopy_df_test")

        expected = [
            (1, "x", pd.to_datetime("2011-01-01").date()),
            (2, "y", pd.to_datetime("2001-04-02").date()),
        ]

        assert len(expected) == len(results)
        for i, result in enumerate(results):
            assert result[0] == expected[i][0]
            assert result[1] == expected[i][1]
            assert result[2] == expected[i][2]

        redshift.insert_dataframe_to_table(
            TEST_DF_2, "locopy_test_2", create=True, batch_size=3
        )
        redshift.execute("SELECT col1, col2 FROM locopy_test_2 ORDER BY col1 ASC")
        results = redshift.cursor.fetchall()
        redshift.execute("drop table if exists locopy_test_2")

        expected = [
            (1, "a"),
            (2, "b"),
            (3, "c"),
            (4, "d"),
            (5, "e"),
            (6, "f"),
            (7, "g"),
        ]

        assert len(expected) == len(results)
        for i, result in enumerate(results):
            assert result[0] == expected[i][0]
            assert result[1] == expected[i][1]

        from decimal import Decimal

        TEST_DF_3 = pd.DataFrame(
            {
                "a": [1, 2],
                "b": [pd.to_datetime("2013-01-01"), pd.to_datetime("2019-01-01")],
                "c": [True, False],
                "d": [Decimal(2), Decimal(3)],
                "e": [None, "x'y"],
            }
        )
        redshift.insert_dataframe_to_table(TEST_DF_3, "locopy_test_3", create=True)
        redshift.execute("SELECT a, b, c, d, e FROM locopy_test_3 ORDER BY a ASC")
        results = redshift.cursor.fetchall()
        redshift.execute("drop table if exists locopy_test_3")

        expected = [
            (1, pd.to_datetime("2013-01-01"), True, 2, None),
            (2, pd.to_datetime("2019-01-01"), False, 3, "x'y"),
        ]

        assert len(expected) == len(results)
        for i, result in enumerate(results):
            assert result[0] == expected[i][0]
            assert result[1] == expected[i][1]
            assert result[2] == expected[i][2]
            assert result[3] == expected[i][3]
            assert result[4] == expected[i][4]
