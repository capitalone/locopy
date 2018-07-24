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
from locopy.utility import compress_file, split_file
from locopy.errors import (CompressionError, LocopySplitError,
                           RedshiftCredentialsError)
import locopy.utility as util



GOOD_CONFIG_YAML = u"""host: my.redshift.cluster.com
port: 1234
dbname: db
user: userid
password: pass"""

BAD_CONFIG_YAML = """port: 1234
dbname: db
user: userid
password: pass"""


def cleanup(splits):
    for file in splits:
        os.remove(file)


def compare_file_contents(base_file, check_files):
    check_files = cycle([open(x, "rb") for x in check_files])
    with open(base_file, 'rb') as base:
        for line in base:
            cfile = next(check_files)
            compare_line = cfile.readline()
            if compare_line != line:
                return False
    return True


@mock.patch('locopy.utility.open')
@mock.patch('locopy.utility.gzip.open')
@mock.patch('locopy.utility.shutil.copyfileobj')
def test_compress_file(mock_shutil, mock_gzip_open, mock_open):
    compress_file('input', 'output')
    mock_open.assert_called_with('input', 'rb')
    mock_gzip_open.assert_called_with('output', 'wb')
    mock_shutil.assert_called_with(mock_open().__enter__(),
                                   mock_gzip_open().__enter__())



@mock.patch('locopy.utility.open')
@mock.patch('locopy.utility.gzip.open')
@mock.patch('locopy.utility.shutil.copyfileobj')
def test_compress_file_exception(
    mock_shutil, mock_gzip_open, mock_open):
    mock_shutil.side_effect = Exception("SomeException")
    with pytest.raises(CompressionError):
        compress_file('input', 'output')




def test_split_file():
    input_file = 'tests/data/mock_file.txt'
    output_file = 'tests/data/mock_output_file.txt'

    expected = ['tests/data/mock_output_file.txt.0',
                'tests/data/mock_output_file.txt.1']
    splits = split_file(input_file, output_file)
    assert splits == expected
    assert compare_file_contents(input_file, expected)
    cleanup(splits)


    expected = ['tests/data/mock_output_file.txt.0',
                'tests/data/mock_output_file.txt.1',
                'tests/data/mock_output_file.txt.2']
    splits = split_file(input_file, output_file, 3)
    assert splits == expected
    assert compare_file_contents(input_file, expected)
    cleanup(splits)


    expected = ['tests/data/mock_output_file.txt.0',
                'tests/data/mock_output_file.txt.1',
                'tests/data/mock_output_file.txt.2',
                'tests/data/mock_output_file.txt.3',
                'tests/data/mock_output_file.txt.4']
    splits = split_file(input_file, output_file, 5)
    assert splits == expected
    assert compare_file_contents(input_file, expected)
    cleanup(splits)



def test_split_file_exception():
    input_file = 'tests/data/mock_file.txt'
    output_file = 'tests/data/mock_output_file.txt'

    if sys.version_info.major == 3:
        builtin_module_name = 'builtins'
    else:
        builtin_module_name = '__builtin__'

    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, -1)
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, 0)
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, 1)
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, 5.65)
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, "123")
    with pytest.raises(LocopySplitError):
        split_file(input_file, output_file, "Test")


    with mock.patch('{0}.next'.format(builtin_module_name)) as mock_next:
        with mock.patch('os.remove') as mock_remove:
            mock_next.side_effect = Exception('SomeException')

            with pytest.raises(LocopySplitError):
                split_file(input_file, output_file)
            assert mock_remove.call_count == 2
            mock_remove.reset_mock()

            with pytest.raises(LocopySplitError):
                split_file(input_file, output_file, 3)
            assert mock_remove.call_count == 3

    cleanup(['tests/data/mock_output_file.txt.0',
             'tests/data/mock_output_file.txt.1',
             'tests/data/mock_output_file.txt.2'])



@mock.patch('locopy.utility.open', mock.mock_open(read_data=GOOD_CONFIG_YAML))
def test_get_redshift_yaml_good():
    actual = util.get_redshift_yaml('filename.yml')
    assert set(actual.keys()) == set(['host','port','dbname','user','password'])
    assert actual['host'] == 'my.redshift.cluster.com'
    assert actual['port'] == 1234
    assert actual['dbname'] == 'db'
    assert actual['user'] == 'userid'
    assert actual['password'] == 'pass'



def test_get_redshift_yaml_io():
    actual = util.get_redshift_yaml(StringIO(GOOD_CONFIG_YAML))
    assert set(actual.keys()) == set(['host','port','dbname','user','password'])
    assert actual['host'] == 'my.redshift.cluster.com'
    assert actual['port'] == 1234
    assert actual['dbname'] == 'db'
    assert actual['user'] == 'userid'
    assert actual['password'] == 'pass'




@mock.patch('locopy.utility.open', mock.mock_open(read_data=BAD_CONFIG_YAML))
def test_get_redshift_yaml_bad():
    with pytest.raises(RedshiftCredentialsError):
        util.get_redshift_yaml('filename.yml')


def test_get_redshift_yaml_no_file():
    with pytest.raises(RedshiftCredentialsError):
        util.get_redshift_yaml('file_that_does_not_exist.yml')


def test_validate_redshift_attributes_good():
    assert util.validate_redshift_attributes(
        host='host', port=1, dbname='db', user='hi', password='nope') is None


def test_validate_redshift_attributes_nones():
    with pytest.raises(RedshiftCredentialsError):
        util.validate_redshift_attributes()
    with pytest.raises(RedshiftCredentialsError):
        util.validate_redshift_attributes(
            {'host':'host', 'port':1, 'dbname':'db', 'user':'hi'})
    with pytest.raises(RedshiftCredentialsError):
        util.validate_redshift_attributes(
            {'host':'host', 'port':1, 'dbname':'db', 'password':'nope'})
    with pytest.raises(RedshiftCredentialsError):
        util.validate_redshift_attributes(
            {'host':'host', 'port':1, 'user':'hi', 'password':'nope'})
    with pytest.raises(RedshiftCredentialsError):
        util.validate_redshift_attributes(
            {'host':'host', 'dbname':'db', 'user':'hi', 'password':'nope'})
    with pytest.raises(RedshiftCredentialsError):
        util.validate_redshift_attributes(
            {'port':1, 'dbname':'db', 'user':'hi', 'password':'nope'})
