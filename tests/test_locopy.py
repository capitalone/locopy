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

import pytest
import pg8000, psycopg2
import locopy

from unittest import mock
from tests import CmdNoConnect
from locopy.errors import (RedshiftCredentialsError, RedshiftConnectionError,
                           RedshiftDisconnectionError, RedshiftError)


GOOD_CONFIG_YAML = """
host: host
port: 1234
dbname: db
user: id
password: pass"""


DBAPIS = [pg8000, psycopg2]



@pytest.mark.parametrize('dbapi', DBAPIS)
def test_constructor(rs_creds, dbapi):
    r = CmdNoConnect(dbapi=dbapi, **rs_creds)
    assert r.host == 'host'
    assert r.port == 'port'
    assert r.dbname == 'dbname'
    assert r.user == 'user'
    assert r.password == 'password'



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('locopy.utility.open', mock.mock_open(read_data=GOOD_CONFIG_YAML))
def test_constructor_yaml(dbapi):
    r = CmdNoConnect(dbapi=dbapi, config_yaml='some_config.yml')
    assert r.host == 'host'
    assert r.port == 1234
    assert r.dbname == 'db'
    assert r.user =='id'
    assert r.password == 'pass'


@pytest.mark.parametrize('dbapi', DBAPIS)
def test_validate_fields(
    rs_bad_1, rs_bad_2, rs_bad_3, rs_bad_4, rs_bad_5, dbapi):
    with pytest.raises(RedshiftCredentialsError):
        CmdNoConnect(dbapi=dbapi, **rs_bad_1)
    with pytest.raises(RedshiftCredentialsError):
        CmdNoConnect(dbapi=dbapi, **rs_bad_2)
    with pytest.raises(RedshiftCredentialsError):
        CmdNoConnect(dbapi=dbapi, **rs_bad_3)
    with pytest.raises(RedshiftCredentialsError):
        CmdNoConnect(dbapi=dbapi, **rs_bad_4)
    with pytest.raises(RedshiftCredentialsError):
        CmdNoConnect(dbapi=dbapi, **rs_bad_5)



@pytest.mark.parametrize('dbapi', DBAPIS)
def test_get_redshift(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        locopy.Cmd(dbapi=dbapi, **rs_creds)

    if dbapi.__name__ == 'pg8000':
        mock_connect.assert_called_with(
            host='host', user='user', port='port', password='password',
            database='dbname', ssl=True)
    else:
        mock_connect.assert_called_with(
            host='host', user='user', port='port', password='password',
            database='dbname', sslmode='require')



@pytest.mark.parametrize('dbapi', DBAPIS)
def test_redshift_connect(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        test_redshift = locopy.Cmd(dbapi=dbapi, **rs_creds)

    if dbapi.__name__ == 'pg8000':
        mock_connect.assert_called_with(
            host='host', user='user', port='port', password='password',
            database='dbname', ssl=True)
    else:
        mock_connect.assert_called_with(
            host='host', user='user', port='port', password='password',
            database='dbname', sslmode='require')

    test_redshift.conn.cursor.assert_called_with()

    # side effect exception
    mock_connect.side_effect = Exception('Connect Exception')
    with pytest.raises(RedshiftConnectionError):
        test_redshift._connect()


@pytest.mark.parametrize('dbapi', DBAPIS)
def test_redshift_disconnect(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        test_redshift = locopy.Cmd(dbapi=dbapi, **rs_creds)
    test_redshift._disconnect()
    test_redshift.conn.close.assert_called_with()
    test_redshift.cursor.close.assert_called_with()

    # side effect exception
    test_redshift.conn.close.side_effect = Exception('Disconnect Exception')
    with pytest.raises(RedshiftDisconnectionError):
        test_redshift._disconnect()



@pytest.mark.parametrize('dbapi', DBAPIS)
def test_is_connected(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        test_redshift = CmdNoConnect(dbapi=dbapi, **rs_creds)
    assert test_redshift._is_connected() is False

    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        test_redshift = locopy.Cmd(dbapi=dbapi, **rs_creds)
    assert test_redshift._is_connected() is True



@pytest.mark.parametrize('dbapi', DBAPIS)
def test_execute(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        with locopy.Cmd(dbapi=dbapi, **rs_creds) as test:
            test.execute('SELECT * FROM some_table')
            assert test.cursor.execute.called is True



@pytest.mark.parametrize('dbapi', DBAPIS)
def test_execute_no_connection_exception(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        test = locopy.Cmd(dbapi=dbapi, **rs_creds)
    test.conn = None
    test.cursor = None

    with pytest.raises(RedshiftConnectionError):
        test.execute('SELECT * FROM some_table')



@pytest.mark.parametrize('dbapi', DBAPIS)
def test_execute_sql_exception(rs_creds, dbapi):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        with locopy.Cmd(dbapi=dbapi, **rs_creds) as test:
            test.cursor.execute.side_effect = Exception('SQL Exception')
            with pytest.raises(RedshiftError):
                test.execute('SELECT * FROM some_table')



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('pandas.DataFrame')
def test_to_dataframe_all(mock_pandas, dbapi, rs_creds):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchall.return_value = [(1,2),(2,3),(3,)]
        with locopy.Cmd(dbapi=dbapi, **rs_creds) as cmd:
            cmd.execute("SELECT 'hello world' AS fld")
            df = cmd.to_dataframe()

    assert mock_connect.return_value.cursor.return_value.fetchall.called
    mock_pandas.assert_called_with(cmd.cursor.fetchall(), columns=[])



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('pandas.DataFrame')
def test_to_dataframe_custom_size(mock_pandas, dbapi, rs_creds):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchmany.return_value = [(1,2),(2,3),(3,)]
        with locopy.Cmd(dbapi=dbapi, **rs_creds) as cmd:
            cmd.execute("SELECT 'hello world' AS fld")
            df = cmd.to_dataframe(size=5)

    mock_connect.return_value.cursor.return_value.fetchmany.assert_called_with(5)
    mock_pandas.assert_called_with(cmd.cursor.fetchmany(), columns=[])



@pytest.mark.parametrize('dbapi', DBAPIS)
@mock.patch('pandas.DataFrame')
def test_to_dataframe_none(mock_pandas, dbapi, rs_creds):
    with mock.patch(dbapi.__name__ + '.connect') as mock_connect:
        mock_connect.return_value.cursor.return_value.fetchmany.return_value = []
        with locopy.Cmd(dbapi=dbapi, **rs_creds) as cmd:
            cmd.execute("SELECT 'hello world' WHERE 1=0")
            assert cmd.to_dataframe(size=5) is None
            mock_pandas.assert_not_called()
