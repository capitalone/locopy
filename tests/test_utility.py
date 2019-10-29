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
import sys
import pytest

from unittest import mock
from io import StringIO
from itertools import cycle
from botocore.credentials import Credentials
from locopy.utility import compress_file, compress_file_list, split_file, concatenate_files, find_column_type
from locopy.errors import CompressionError, LocopySplitError, CredentialsError, LocopyConcatError
import locopy.utility as util


GOOD_CONFIG_YAML = u"""host: my.redshift.cluster.com
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
    check_files = cycle([open(x, "rb") for x in check_files])
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
    mock_shutil.assert_called_with(mock_open().__enter__(), mock_gzip_open().__enter__())


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
def test_compress_file_list_exception(mock_shutil, mock_gzip_open, mock_open, mock_remove):
    mock_shutil.side_effect = Exception("SomeException")
    with pytest.raises(CompressionError):
        compress_file_list(["input1", "input2"])


def test_split_file():
    input_file = "tests/data/mock_file.txt"
    output_file = "tests/data/mock_output_file.txt"

    splits = split_file(input_file, output_file)
    assert splits == [input_file]

    expected = ["tests/data/mock_output_file.txt.0", "tests/data/mock_output_file.txt.1"]
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

    with mock.patch("{0}.next".format(builtin_module_name)) as mock_next:
        with mock.patch("os.remove") as mock_remove:
            mock_next.side_effect = Exception("SomeException")

            with pytest.raises(LocopySplitError):
                split_file(input_file, output_file, 2)
            assert mock_remove.call_count == 2
            mock_remove.reset_mock()

            with pytest.raises(LocopySplitError):
                split_file(input_file, output_file, 3)
            assert mock_remove.call_count == 3

    cleanup(
        [
            "tests/data/mock_output_file.txt.0",
            "tests/data/mock_output_file.txt.1",
            "tests/data/mock_output_file.txt.2",
        ]
    )


@mock.patch("locopy.utility.open", mock.mock_open(read_data=GOOD_CONFIG_YAML))
def test_read_config_yaml_good():
    actual = util.read_config_yaml("filename.yml")
    assert set(actual.keys()) == set(["host", "port", "database", "user", "password"])
    assert actual["host"] == "my.redshift.cluster.com"
    assert actual["port"] == 1234
    assert actual["database"] == "db"
    assert actual["user"] == "userid"
    assert actual["password"] == "pass"


def test_read_config_yaml_io():
    actual = util.read_config_yaml(StringIO(GOOD_CONFIG_YAML))
    assert set(actual.keys()) == set(["host", "port", "database", "user", "password"])
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
        assert [int(line.rstrip("\n")) for line in open(output)] == list(range(1, 16))
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

    import pandas as pd
    # add timestamp
    input_text = pd.DataFrame.from_dict({
        'a': [1, 2, 3],
        'b': ['x', 'y', 'z'],
        'c': ['2019-91-01', '2011-01-01', '2011-10-01'],
        'd': ['2011-01-01', '2001-04-02', '2019-04-23'],
        'e': [1.2, 2.44, 4.23 ],
        'f': [11, 14, None],
        'g': [None, None, None],
        'h': [pd.to_datetime('2019-01-01'), pd.to_datetime('2019-03-01'), pd.to_datetime('2019-04-01')]

    })
    output_text = {'a': 'int',
                   'b': 'varchar',
                   'c': 'varchar',
                   'd': 'date',
                   'e': 'float',
                   'f': 'float',
                   'g': 'varchar',
                   'h': 'timestamp'
                  }
    assert find_column_type(input_text) == output_text
