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

import datetime
import os
import sys
from io import StringIO
from itertools import cycle
from pathlib import Path
from unittest import mock

import locopy.utility as util
import polars as pl
import pyarrow as pa
import pytest
from locopy.errors import (
    CompressionError,
    CredentialsError,
    LocopyConcatError,
    LocopyIgnoreHeaderError,
    LocopySplitError,
)
from locopy.utility import (
    compress_file,
    compress_file_list,
    concatenate_files,
    find_column_type,
    get_ignoreheader_number,
    split_file,
)

GOOD_CONFIG_YAML = """host: my.redshift.cluster.com
port: 1234
database: db
user: userid
password: pass"""

BAD_CONFIG_YAML = """port: 1234
database: db
user: userid
password: pass"""


def cleanup(splits):
    for file in splits:
        os.remove(file)


def compare_file_contents(base_file, check_files):
    check_files = cycle([open(x, "rb") for x in check_files])  # noqa: SIM115
    with open(base_file, "rb") as base:
        for line in base:
            cfile = next(check_files)
            compare_line = cfile.readline()
            if compare_line != line:
                return False
    return True


@mock.patch("locopy.utility.open")
@mock.patch("locopy.utility.gzip.open")
@mock.patch("locopy.utility.shutil.copyfileobj")
def test_compress_file(mock_shutil, mock_gzip_open, mock_open):
    compress_file("input", "output")
    mock_open.assert_called_with("input", "rb")
    mock_gzip_open.assert_called_with("output", "wb")
    mock_shutil.assert_called_with(
        mock_open().__enter__(), mock_gzip_open().__enter__()
    )


@mock.patch("locopy.utility.open")
@mock.patch("locopy.utility.gzip.open")
@mock.patch("locopy.utility.shutil.copyfileobj")
def test_compress_file_exception(mock_shutil, mock_gzip_open, mock_open):
    mock_shutil.side_effect = Exception("SomeException")
    with pytest.raises(CompressionError):
        compress_file("input", "output")


@mock.patch("locopy.utility.os.remove")
@mock.patch("locopy.utility.open")
@mock.patch("locopy.utility.gzip.open")
@mock.patch("locopy.utility.shutil.copyfileobj")
def test_compress_file_list(mock_shutil, mock_gzip_open, mock_open, mock_remove):
    res = compress_file_list([])
    assert res == []
    res = compress_file_list(["input1"])
    assert res == ["input1.gz"]
    res = compress_file_list(["input1", "input2"])
    assert res == ["input1.gz", "input2.gz"]


@mock.patch("locopy.utility.os.remove")
@mock.patch("locopy.utility.open")
@mock.patch("locopy.utility.gzip.open")
@mock.patch("locopy.utility.shutil.copyfileobj")
def test_compress_file_list_exception(
    mock_shutil, mock_gzip_open, mock_open, mock_remove
):
    mock_shutil.side_effect = Exception("SomeException")
    with pytest.raises(CompressionError):
        compress_file_list(["input1", "input2"])


def test_split_file():
    input_file = "tests/data/mock_file.txt"
    output_file = "tests/data/mock_output_file.txt"

    splits = split_file(input_file, output_file)
    assert splits == [input_file]

    expected = [
        "tests/data/mock_output_file.txt.0",
        "tests/data/mock_output_file.txt.1",
    ]
    splits = split_file(input_file, output_file, 2)
    assert splits == expected
    assert compare_file_contents(input_file, expected)
    cleanup(splits)

    expected = [
        "tests/data/mock_output_file.txt.0",
        "tests/data/mock_output_file.txt.1",
        "tests/data/mock_output_file.txt.2",
    ]
    splits = split_file(input_file, output_file, 3)
    assert splits == expected
    assert compare_file_contents(input_file, expected)
    cleanup(splits)

    expected = [
        "tests/data/mock_output_file.txt.0",
        "tests/data/mock_output_file.txt.1",
        "tests/data/mock_output_file.txt.2",
        "tests/data/mock_output_file.txt.3",
        "tests/data/mock_output_file.txt.4",
    ]
    splits = split_file(input_file, output_file, 5)
    assert splits == expected
    assert compare_file_contents(input_file, expected)
    cleanup(splits)


def test_split_file_header():
    input_file = "tests/data/mock_file_header.txt"
    input_file_no_header = "tests/data/mock_file.txt"
    output_file = "tests/data/mock_output_file_header.txt"

    splits = split_file(input_file, output_file, ignore_header=1)
    assert splits == [input_file]

    expected = [
        "tests/data/mock_output_file_header.txt.0",
        "tests/data/mock_output_file_header.txt.1",
    ]
    splits = split_file(input_file, output_file, 2, ignore_header=1)
    assert splits == expected
    assert compare_file_contents(input_file_no_header, expected)
    cleanup(splits)

    expected = [
        "tests/data/mock_output_file_header.txt.0",
        "tests/data/mock_output_file_header.txt.1",
        "tests/data/mock_output_file_header.txt.2",
    ]
    splits = split_file(input_file, output_file, 3, ignore_header=1)
    assert splits == expected
    assert compare_file_contents(input_file_no_header, expected)
    cleanup(splits)

    expected = [
        "tests/data/mock_output_file_header.txt.0",
        "tests/data/mock_output_file_header.txt.1",
        "tests/data/mock_output_file_header.txt.2",
        "tests/data/mock_output_file_header.txt.3",
        "tests/data/mock_output_file_header.txt.4",
    ]
    splits = split_file(input_file, output_file, 5, ignore_header=1)
    assert splits == expected
    assert compare_file_contents(input_file_no_header, expected)
    cleanup(splits)


def test_split_file_exception():
    input_file = "tests/data/mock_file.txt"
    output_file = "tests/data/mock_output_file.txt"

    if sys.version_info.major == 3:
        builtin_module_name = "builtins"
    else:
        builtin_module_name = "__builtin__"

    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, -1)
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, 0)
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, 5.65)
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, "123")
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, "Test")

    with mock.patch(f"{builtin_module_name}.next") as mock_next:
        mock_next.side_effect = Exception("SomeException")

        with pytest.raises(LocopySplitError):
            split_file(input_file, output_file, 2)
        assert not Path("tests/data/mock_output_file.txt.0").exists()
        assert not Path("tests/data/mock_output_file.txt.1").exists()

        with pytest.raises(LocopySplitError):
            split_file(input_file, output_file, 3)
        assert not Path("tests/data/mock_output_file.txt.0").exists()
        assert not Path("tests/data/mock_output_file.txt.1").exists()
        assert not Path("tests/data/mock_output_file.txt.2").exists()


@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
def test_read_config_yaml_good():
    actual = util.read_config_yaml("filename.yml")
    assert set(actual.keys()) == {"host", "port", "database", "user", "password"}
    assert actual["host"] == "my.redshift.cluster.com"
    assert actual["port"] == 1234
    assert actual["database"] == "db"
    assert actual["user"] == "userid"
    assert actual["password"] == "pass"


def test_read_config_yaml_io():
    actual = util.read_config_yaml(StringIO(GOOD_CONFIG_YAML))
    assert set(actual.keys()) == {"host", "port", "database", "user", "password"}
    assert actual["host"] == "my.redshift.cluster.com"
    assert actual["port"] == 1234
    assert actual["database"] == "db"
    assert actual["user"] == "userid"
    assert actual["password"] == "pass"


def test_read_config_yaml_no_file():
    with pytest.raises(CredentialsError):
        util.read_config_yaml("file_that_does_not_exist.yml")


def test_concatenate_files():
    inputs = ["tests/data/cat_1.txt", "tests/data/cat_2.txt", "tests/data/cat_3.txt"]
    output = "tests/data/cat_output.txt"
    with mock.patch("locopy.utility.os.remove") as mock_remove:
        concatenate_files(inputs, output)
        assert mock_remove.call_count == 3
        with open(output) as f:
            assert [int(line.rstrip("\n")) for line in f] == list(range(1, 16))
    os.remove(output)


def test_concatenate_files_exception():
    inputs = ["tests/data/cat_1.txt", "tests/data/cat_2.txt", "tests/data/cat_3.txt"]
    output = "tests/data/cat_output.txt"
    with pytest.raises(LocopyConcatError):
        concatenate_files([], output, remove=False)

    with mock.patch("locopy.utility.open") as mock_open:
        mock_open.side_effect = Exception()
        with pytest.raises(LocopyConcatError):
            concatenate_files(inputs, output, remove=False)


def test_find_column_type():
    from decimal import Decimal

    import pandas as pd

    # add timestamp
    input_text = pd.DataFrame.from_dict(
        {
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
            "c": ["2019-91-01", "2011-01-01", "2011-10-01"],
            "d": ["2011-01-01", "2001-04-02", "2019-04-23"],
            "e": [1.2, 2.44, 4.23],
            "f": [11, 14, None],
            "g": [None, None, None],
            "h": [
                pd.to_datetime("2019-01-01"),
                pd.to_datetime("2019-03-01"),
                pd.to_datetime("2019-04-01"),
            ],
            "i": [None, "2011-04-02", "2002-04-23"],
            "j": [None, "2011-01-01 12:11:02", "2022-03-02 23:59:59"],
            "k": [Decimal("3.3"), Decimal(100), None],
            "l": pd.Series([1, 2, 3], dtype="category"),
            "m": ["2022-02", "2022-03", "2020-02"],
            "n": ["2020q1", "2021q2", "2022q3"],
            "o": ["10-DEC-2022", "11-NOV-2020", "10-OCT-2020"],
        }
    )
    output_text_snowflake = {
        "a": "int",
        "b": "varchar",
        "c": "varchar",
        "d": "date",
        "e": "float",
        "f": "float",
        "g": "varchar",
        "h": "timestamp",
        "i": "date",
        "j": "timestamp",
        "k": "float",
        "l": "varchar",
        "m": "varchar",
        "n": "varchar",
        "o": "date",
    }
    output_text_redshift = {
        "a": "int",
        "b": "varchar",
        "c": "varchar",
        "d": "date",
        "e": "float",
        "f": "float",
        "g": "varchar",
        "h": "timestamp",
        "i": "date",
        "j": "timestamp",
        "k": "float",
        "l": "varchar",
        "m": "date",
        "n": "date",
        "o": "date",
    }
    assert find_column_type(input_text, "snowflake") == output_text_snowflake
    assert find_column_type(input_text, "redshift") == output_text_redshift


def test_find_column_type_new():
    import pandas as pd

    input_text = pd.DataFrame.from_dict(
        {
            "a": [1],
            "b": [pd.Timestamp("2017-01-01T12+0")],
            "c": [1.2],
            "d": ["a"],
            "e": [True],
        }
    )

    input_text = input_text.astype(
        dtype={
            "a": pd.Int64Dtype(),
            "b": pd.DatetimeTZDtype(tz=datetime.timezone.utc),
            "c": pd.Float64Dtype(),
            "d": pd.StringDtype(),
            "e": pd.BooleanDtype(),
        }
    )

    output_text_snowflake = {
        "a": "int",
        "b": "timestamp",
        "c": "float",
        "d": "varchar",
        "e": "boolean",
    }

    output_text_redshift = {
        "a": "int",
        "b": "timestamp",
        "c": "float",
        "d": "varchar",
        "e": "boolean",
    }
    assert find_column_type(input_text, "snowflake") == output_text_snowflake
    assert find_column_type(input_text, "redshift") == output_text_redshift


def test_find_column_type_pyarrow():
    import pandas as pd

    input_text = pd.DataFrame.from_dict(
        {
            "a": [1],
            "b": [pd.Timestamp("2017-01-01T12+0")],
            "c": [1.2],
            "d": ["a"],
            "e": [True],
        }
    )

    input_text = input_text.astype(
        dtype={
            "a": "int64[pyarrow]",
            "b": pd.ArrowDtype(pa.timestamp("ns", tz="UTC")),
            "c": "float64[pyarrow]",
            "d": pd.ArrowDtype(pa.string()),
            "e": "bool[pyarrow]",
        }
    )

    output_text_snowflake = {
        "a": "int",
        "b": "timestamp",
        "c": "float",
        "d": "varchar",
        "e": "boolean",
    }

    output_text_redshift = {
        "a": "int",
        "b": "timestamp",
        "c": "float",
        "d": "varchar",
        "e": "boolean",
    }
    assert find_column_type(input_text, "snowflake") == output_text_snowflake
    assert find_column_type(input_text, "redshift") == output_text_redshift


def test_find_column_type_polars():
    input_text = pl.DataFrame(
        {
            "a": [1],
            "b": ["2022-03-02 23:59:59"],
            "c": [1.2],
            "d": ["a"],
            "e": [True],
            "f": ["2011-01-01"],
            "g": [
                "10-DEC-2022"
            ],  # remains as string, otherwise polars will convert it to `2022-12-10`
            "h": ["11:35:49"],
            "i": [30],
            "j": [2025],
        }
    )

    input_text = input_text.with_columns(
        [
            pl.date(
                pl.col("j"),
                pl.col("a"),
                pl.col("i"),  # create date of 2025-01-30
            ).alias("k"),
            pl.datetime(
                pl.col("j"),
                pl.col("a"),
                pl.col("i"),
                pl.col("a"),
                pl.col("i"),
                time_zone="Australia/Sydney",
                # create datetime of 2025-01-30 01:30pm Australia time
            ).alias("l"),
            pl.time(
                12,
                35,
                15,  # create 12:35:15pm
            ).alias("m"),
        ]
    )

    output_text_snowflake = {
        "a": "int",
        "b": "timestamp",
        "c": "float",
        "d": "varchar",
        "e": "boolean",
        "f": "date",
        "g": "varchar",
        "h": "time",
        "i": "int",
        "j": "int",
        "k": "date",
        "l": "timestamp",
        "m": "timestamp",
    }

    output_text_redshift = {
        "a": "int",
        "b": "timestamp",
        "c": "float",
        "d": "varchar",
        "e": "boolean",
        "f": "date",
        "g": "varchar",
        "h": "time",
        "i": "int",
        "j": "int",
        "k": "date",
        "l": "timestamp",
        "m": "timestamp",
    }
    assert find_column_type(input_text, "snowflake") == output_text_snowflake
    assert find_column_type(input_text, "redshift") == output_text_redshift


def test_get_ignoreheader_number():
    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "IGNOREHEADER as 1",
            ]
        )
        == 1
    )
    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "IGNOREHEADER as 2",
            ]
        )
        == 2
    )
    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "IGNOREHEADER as 99",
            ]
        )
        == 99
    )

    assert (
        get_ignoreheader_number(
            ["DATEFORMAT 'auto'", "COMPUPDATE ON", "TRUNCATECOLUMNS", "IGNOREHEADER 1"]
        )
        == 1
    )
    assert (
        get_ignoreheader_number(
            ["DATEFORMAT 'auto'", "COMPUPDATE ON", "TRUNCATECOLUMNS", "IGNOREHEADER 2"]
        )
        == 2
    )
    assert (
        get_ignoreheader_number(
            ["DATEFORMAT 'auto'", "COMPUPDATE ON", "TRUNCATECOLUMNS", "IGNOREHEADER 99"]
        )
        == 99
    )

    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "IGNOREHEADER is 1",
            ]
        )
        == 1
    )
    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "IGNOREHEADER is 2",
            ]
        )
        == 2
    )
    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "IGNOREHEADER is 99",
            ]
        )
        == 99
    )

    assert (
        get_ignoreheader_number(
            ["DATEFORMAT 'auto'", "COMPUPDATE ON", "TRUNCATECOLUMNS"]
        )
        == 0
    )
    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "IGNOREHEADERAS 2",
            ]
        )
        == 0
    )
    assert (
        get_ignoreheader_number(
            [
                "DATEFORMAT 'auto'",
                "COMPUPDATE ON",
                "TRUNCATECOLUMNS",
                "SOMETHINGIGNOREHEADER AS 2",
            ]
        )
        == 0
    )
    assert get_ignoreheader_number([]) == 0

    with pytest.raises(LocopyIgnoreHeaderError):
        get_ignoreheader_number(["IGNOREHEADER 1", "IGNOREHEADER 99"])
