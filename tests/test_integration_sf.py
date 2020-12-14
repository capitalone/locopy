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
import shutil
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pytest
import snowflake.connector

import locopy

DBAPIS = [snowflake.connector]
INTEGRATION_CREDS = str(Path.home()) + os.sep + ".locopy-sfrc"
S3_BUCKET = "locopy-integration-testing"
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_FILE = os.path.join(CURR_DIR, "data", "mock_file.txt")
LOCAL_FILE_JSON = os.path.join(CURR_DIR, "data", "mock_file.json")
LOCAL_FILE_DL = os.path.join(CURR_DIR, "data", "mock_file_dl.txt")
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
def test_snowflake_execute_single_rows(dbapi):

    expected = pd.DataFrame({"field_1": [1], "field_2": [2]})
    with locopy.Snowflake(dbapi=dbapi, **CREDS_DICT) as test:
        test.execute("SELECT 1 AS field_1, 2 AS field_2 ")
        df = test.to_dataframe()
        df.columns = [c.lower() for c in df.columns]

    assert np.allclose(df["field_1"], expected["field_1"])


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_snowflake_execute_multiple_rows(dbapi):

    expected = pd.DataFrame({"field_1": [1, 2], "field_2": [1, 2]})
    with locopy.Snowflake(dbapi=dbapi, **CREDS_DICT) as test:
        test.execute(
            "SELECT 1 AS field_1, 1 AS field_2 " "UNION " "SELECT 2 AS field_1, 2 AS field_2"
        )
        df = test.to_dataframe()
        df.columns = [c.lower() for c in df.columns]

    assert np.allclose(df["field_1"], expected["field_1"])
    assert np.allclose(df["field_2"], expected["field_2"])


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_upload_download_internal(dbapi):

    with locopy.Snowflake(dbapi=dbapi, **CREDS_DICT) as test:
        # delete if exists
        test.execute("REMOVE @~/staged/mock_file_dl.txt")

        # test
        shutil.copy(LOCAL_FILE, LOCAL_FILE_DL)
        test.upload_to_internal(LOCAL_FILE_DL, "@~/staged/", auto_compress=False)
        test.execute("LIST @~/staged/mock_file_dl.txt")
        res = test.cursor.fetchall()
        assert res[0][0] == "staged/mock_file_dl.txt"

        test.download_from_internal(
            "@~/staged/mock_file_dl.txt", os.path.dirname(LOCAL_FILE_DL) + os.sep
        )
        assert filecmp.cmp(LOCAL_FILE, LOCAL_FILE_DL)

        # clean up
        test.execute("REMOVE @~/staged/mock_file_dl.txt")
        os.remove(LOCAL_FILE_DL)


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_copy(dbapi):

    with locopy.Snowflake(dbapi=dbapi, **CREDS_DICT) as test:
        test.upload_to_internal(LOCAL_FILE, "@~/staged/")
        test.execute("USE SCHEMA {}".format(CREDS_DICT["schema"]))
        test.execute(
            "CREATE OR REPLACE TEMPORARY TABLE locopy_integration_testing (id INTEGER, variable VARCHAR(20))"
        )
        test.copy(
            "locopy_integration_testing",
            "@~/staged/mock_file.txt.gz",
            copy_options=["PURGE = TRUE"],
        )
        test.execute("SELECT * FROM locopy_integration_testing ORDER BY id")
        results = test.cursor.fetchall()

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
def test_copy_json(dbapi):

    with locopy.Snowflake(dbapi=dbapi, **CREDS_DICT) as test:
        test.upload_to_internal(LOCAL_FILE_JSON, "@~/staged/")
        test.execute("USE SCHEMA {}".format(CREDS_DICT["schema"]))
        test.execute(
            "CREATE OR REPLACE TEMPORARY TABLE locopy_integration_testing (variable VARIANT)"
        )
        test.copy(
            "locopy_integration_testing",
            "@~/staged/mock_file.json.gz",
            file_type="json",
            copy_options=["PURGE = TRUE"],
        )
        test.execute(
            "SELECT variable:location:city, variable:price FROM locopy_integration_testing ORDER BY variable"
        )
        results = test.cursor.fetchall()

        expected = [
            ('"Belmont"', '"92567"'),
            ('"Lexington"', '"75836"'),
            ('"Winchester"', '"89921"'),
        ]

        for i, result in enumerate(results):
            assert result[0] == expected[i][0]
            assert result[1] == expected[i][1]


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_to_dataframe(dbapi):

    with locopy.Snowflake(dbapi=dbapi, **CREDS_DICT) as test:
        test.upload_to_internal(LOCAL_FILE_JSON, "@~/staged/")
        test.execute("USE SCHEMA {}".format(CREDS_DICT["schema"]))
        test.execute(
            "CREATE OR REPLACE TEMPORARY TABLE locopy_integration_testing (variable VARIANT)"
        )
        test.copy(
            "locopy_integration_testing",
            "@~/staged/mock_file.json.gz",
            file_type="json",
            copy_options=["PURGE = TRUE"],
        )

        # get all
        test.execute(
            "SELECT variable:location:city, variable:price FROM locopy_integration_testing ORDER BY variable"
        )
        result = test.to_dataframe()
        result.columns = [c.lower() for c in result.columns]
        expected = pd.DataFrame(
            [('"Belmont"', '"92567"'), ('"Lexington"', '"75836"'), ('"Winchester"', '"89921"'),],
            columns=["variable:location:city", "variable:price"],
        )

        assert (result["variable:location:city"] == expected["variable:location:city"]).all()
        assert (result["variable:price"] == expected["variable:price"]).all()

        # with size of 2
        test.execute(
            "SELECT variable:location:city, variable:price FROM locopy_integration_testing ORDER BY variable"
        )
        result = test.to_dataframe(size=2)
        result.columns = [c.lower() for c in result.columns]
        expected = pd.DataFrame(
            [('"Belmont"', '"92567"'), ('"Lexington"', '"75836"'),],
            columns=["variable:location:city", "variable:price"],
        )

        assert (result["variable:location:city"] == expected["variable:location:city"]).all()
        assert (result["variable:price"] == expected["variable:price"]).all()

        # with non-select query
        test.execute("DROP table locopy_integration_testing")
        result = test.to_dataframe()
        assert result["status"][0] == "LOCOPY_INTEGRATION_TESTING successfully dropped."


@pytest.mark.integration
@pytest.mark.parametrize("dbapi", DBAPIS)
def test_insert_dataframe_to_table(dbapi):

    with locopy.Snowflake(dbapi=dbapi, **CREDS_DICT) as test:
        test.insert_dataframe_to_table(TEST_DF, "test", create=True)
        test.execute("SELECT a, b, c FROM test ORDER BY a ASC")
        results = test.cursor.fetchall()
        test.execute("drop table if exists test")

        expected = [
            (1, "x", pd.to_datetime("2011-01-01").date()),
            (2, "y", pd.to_datetime("2001-04-02").date()),
        ]

        assert len(expected) == len(results)
        for i, result in enumerate(results):
            assert result[0] == expected[i][0]
            assert result[1] == expected[i][1]
            assert result[2] == expected[i][2]

        test.insert_dataframe_to_table(TEST_DF_2, "test_2", create=True)
        test.execute("SELECT col1, col2 FROM test_2 ORDER BY col1 ASC")
        results = test.cursor.fetchall()
        test.execute("drop table if exists test_2")

        expected = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"), (7, "g")]

        assert len(expected) == len(results)
        for i, result in enumerate(results):
            assert result[0] == expected[i][0]
            assert result[1] == expected[i][1]

        from decimal import Decimal

        TEST_DF_3 = pd.DataFrame(
            {
                "a": [1, 2],
                "b": [pd.to_datetime("2013-01-01"), pd.to_datetime("2019-01-01")],
                "c": ["1.2", "3.5"],
                "d": [Decimal(2), Decimal(3)],
                "e": pd.Series([0, 1], dtype="category"),
            }
        )
        test.insert_dataframe_to_table(TEST_DF_3, "test_3", create=True)
        test.execute("SELECT a, b, c, d, e FROM test_3 ORDER BY a ASC")
        results = test.cursor.fetchall()
        test.execute("drop table if exists test_3")

        expected = [
            (1, pd.to_datetime("2013-01-01"), 1.2, 2, "0"),
            (2, pd.to_datetime("2019-01-01"), 3.5, 3, "1"),
        ]

        assert len(expected) == len(results)
        for i, result in enumerate(results):
            for j, item in enumerate(result):
                assert result[j] == expected[i][j]
