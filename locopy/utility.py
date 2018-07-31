# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2018 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utility Module
Module which utility functions for use within the application
"""
import threading
import sys
import os
import re
import gzip
import shutil
import yaml
from itertools import cycle
from .logger import (get_logger, DEBUG, INFO, WARN, ERROR, CRITICAL)
from .errors import (CompressionError, LocopySplitError,
                     RedshiftCredentialsError)

logger = get_logger(__name__, INFO)


def write_file(data, delimiter, filepath, mode='w'):
    """Write data to a file.

    Parameters
    ----------
    data : list
        List of lists

    delimiter : str
        Delimiter by which columns will be separated

    filepath : str
        Location of the output file

    mode : str
        File writing mode. Examples include 'w' for write or 'a' for append.
        Defaults to write mode.
        See https://www.tutorialspoint.com/python/python_files_io.htm
    """
    logger.debug('Attempting to write data to file %s' % filepath)
    try:
        with open(filepath, mode) as f:
            for row in data:
                f.write(delimiter.join([str(r) for r in row]) + '\n')
    except Exception as e:
        logger.error("Unable to write file to %s due to err %s" % (filepath, e))
    return


def compress_file(input_file, output_file):
    """Compresses a file (gzip)

    Parameters
    ----------
    input_file : str
        Path to input file to compress
    output_file : str
        Path to write the compressed file
    """
    try:
        with open(input_file, 'rb') as f_in:
            with gzip.open(output_file, 'wb') as f_out:
                logger.info('compressing (gzip): %s to %s',
                            input_file, output_file)
                shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        logger.error('Error compressing the file. err: %s', e)
        raise CompressionError('Error compressing the file.')



def split_file(input_file, output_file, splits=2):
    """Split a file into equal files by lines.

    For example: ``myinputfile.txt`` will be split into ``myoutputfile.txt.01``
    , ```myoutputfile.txt.02`` etc..

    Parameters
    ----------
    input_file : str
        Path to input file to split

    output_file : str
        Name of the output file

    splits : int, optional
        Number of splits to perform. Must be greater than one. Defaults to 2

    Returns
    -------
    list
        List of strings with the file paths of the split files

    Raises
    ------
    LocopySplitError
        If ``splits`` is less than 2 or some processing error when splitting
    """
    if type(splits) is not int or splits < 2:
        logger.error('Number of splits is invalid')
        raise LocopySplitError(
            'Number of splits must be greater than one and an integer.')

    try:
        pool = [x for x in range(splits)]
        cpool = cycle(pool)
        logger.info('splitting file: %s into %s files', input_file, splits)
        # open output file handlers
        files = [open("{0}.{1}".format(output_file, x), "wb") for x in pool]
        # open input file and send line to different handler
        with open(input_file, 'rb') as f_in:
            for line in f_in:
                files[next(cpool)].write(line)
        # close file connection
        for x in pool:
            files[x].close()
        return [f.name for f in files]
    except Exception as e:
        logger.error('Error splitting the file. err: %s', e)
        if len(files) > 0:
            logger.error('Cleaning up intermediary files: %s', files)
            for x in pool:
                files[x].close()
                os.remove(files[x].name)
        raise LocopySplitError('Error splitting the file.')



def get_redshift_yaml(config_yaml):
    """
    Reads a Redshift configuration YAML file to populate the Redshift
    connection attributes, and validate required ones. Example::

        host: my.redshift.cluster.com
        port: 5439
        dbname: db
        user: userid
        password: password

    Parameters
    ----------
    config_yaml : str or file pointer
        String representing the file location of the configuration file, or a
        pointer to an open file object

    Returns
    -------
    dict
        A dictionary of parameters for setting up a connection to Redshift.

    Raises
    ------
    RedshiftCredentialsError
        If any elements are missing from YAML file
    """
    try:
        if isinstance(config_yaml, str):
            with open(config_yaml) as config:
                locopy_yaml = yaml.load(config)
        else:
            locopy_yaml = yaml.load(config_yaml)
    except Exception as e:
        logger.error('Error reading Redshift yaml. err: %s', e)
        raise RedshiftCredentialsError('Error reading Redshift yaml.')
    validate_redshift_attributes(**locopy_yaml)
    return locopy_yaml



def validate_redshift_attributes(
    host=None, port=None, dbname=None, user=None, password=None, **kwargs):
    """Validate Redshift connection attributes to make sure none are missing.

    * All the Redshift connect details need to be set:
        * Host
        * Port
        * Database name
        * Database username
        * Database password

    Raises
    ------
    RedshiftCredentialsError
        If key fields are None.
    """
    if host is None:
        raise RedshiftCredentialsError('Redshift host missing')
    if port is None:
        raise RedshiftCredentialsError('Redshift port missing')
    if dbname is None:
        raise RedshiftCredentialsError('Redshift dbname missing')
    if user is None:
        raise RedshiftCredentialsError('Redshift username missing')
    if password is None:
        raise RedshiftCredentialsError('Redshift password missing')



class ProgressPercentage(object):
    """
    ProgressPercentage class is used by the S3Transfer upload_file callback
    Please see the following url for more information:
    http://boto3.readthedocs.org/en/latest/reference/customizations/s3.html#ref-s3transfer-usage
    """

    def __init__(self, filename):
        """
        Initiate the ProgressPercentage class, using the base information which
        makes up a pipeline
        Args:
            filename (str): A name of the file which we will monitor the
            progress of
        """
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write('\rTransfering [{0}] {1:.2f}%'.format(
                '#'*int(percentage/10), percentage))
            sys.stdout.flush()
