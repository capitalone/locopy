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
import gzip
import os
import shutil
import sys
import threading
from collections import OrderedDict
from itertools import cycle

import yaml

from .errors import CompressionError, CredentialsError, LocopyConcatError, LocopySplitError
from .logger import get_logger, INFO

logger = get_logger(__name__, INFO)


def write_file(data, delimiter, filepath, mode="w"):
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
    logger.debug("Attempting to write data to file: %s", filepath)
    try:
        with open(filepath, mode) as f:
            for row in data:
                f.write(delimiter.join([str(r) for r in row]) + "\n")
    except Exception as e:
        logger.error("Unable to write file to %s due to err: %s", filepath, e)


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
        with open(input_file, "rb") as f_in:
            with gzip.open(output_file, "wb") as f_out:
                logger.info("compressing (gzip): %s to %s", input_file, output_file)
                shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        logger.error("Error compressing the file. err: %s", e)
        raise CompressionError("Error compressing the file.")


def compress_file_list(file_list):
    """Compresses a list of files (gzip) and clean up the old files

    Parameters
    ----------
    file_list : list
        List of strings with the file paths of the files to compress

    Returns
    -------
    list
        List of strings with the file paths of the compressed files (original file name with
        gz appended)
    """
    for i, f in enumerate(file_list):
        gz = "{0}.gz".format(f)
        compress_file(f, gz)
        file_list[i] = gz
        os.remove(f)  # cleanup old files
    return file_list


def split_file(input_file, output_file, splits=1):
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
        Number of splits to perform. Must be greater than zero. Defaults to 1

    Returns
    -------
    list
        List of strings with the file paths of the split files

    Raises
    ------
    LocopySplitError
        If ``splits`` is less than 1 or some processing error when splitting
    """
    if type(splits) is not int or splits <= 0:
        logger.error("Number of splits is invalid")
        raise LocopySplitError("Number of splits must be greater than zero and an integer.")

    if splits == 1:
        return [input_file]

    try:
        pool = list(range(splits))
        cpool = cycle(pool)
        logger.info("splitting file: %s into %s files", input_file, splits)
        # open output file handlers
        files = [open("{0}.{1}".format(output_file, x), "wb") for x in pool]
        # open input file and send line to different handler
        with open(input_file, "rb") as f_in:
            for line in f_in:
                files[next(cpool)].write(line)
        # close file connection
        for x in pool:
            files[x].close()
        return [f.name for f in files]
    except Exception as e:
        logger.error("Error splitting the file. err: %s", e)
        if len(files) > 0:
            logger.error("Cleaning up intermediary files: %s", files)
            for x in pool:
                files[x].close()
                os.remove(files[x].name)
        raise LocopySplitError("Error splitting the file.")


def concatenate_files(input_list, output_file, remove=True):
    """Concatenate a list of files into one file.

    Parameters
    ----------
    input_list : list
        List of strings with the paths to input files to concateneate

    output_file : str
        Path of the output file

    remove: bool, optional
        Removes the files from the input list if ``True``. Defaults to ``True``

    Raises
    ------
    LocopyConcatError
        If ``input_list`` or there is a issue while concatenating the files into one
    """
    if len(input_list) == 0:
        raise LocopyConcatError("Input list is empty.")
    try:
        with open(output_file, "ab") as main_f:
            for f in input_list:
                with open(f, "rb") as temp_f:
                    for line in temp_f:
                        main_f.write(line)
                if remove:  # as we go for space consideration
                    os.remove(f)
    except Exception as e:
        logger.error("Error concateneating files. err: %s", e)
        raise LocopyConcatError("Error concateneating files.")


def read_config_yaml(config_yaml):
    """
    Reads a configuration YAML file to populate the database
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
        A dictionary of parameters for setting up a connection to the database.

    Raises
    ------
    CredentialsError
        If any connection items are missing from the YAML file
    """
    try:
        if isinstance(config_yaml, str):
            with open(config_yaml) as config:
                locopy_yaml = yaml.load(config)
        else:
            locopy_yaml = yaml.load(config_yaml)
    except Exception as e:
        logger.error("Error reading yaml. err: %s", e)
        raise CredentialsError("Error reading yaml.")
    return locopy_yaml


# make it more granular, eg. include length
def find_column_type(dataframe):
    """
    Find data type of each column from the dataframe.

    Parameters
    ----------
    dataframe : Pandas dataframe

    Returns
    -------
    dict
        A dictionary of columns with their data type
    """
    from datetime import datetime, date

    def validate_date(date_text):
        try:
            datetime.strptime(date_text, "%Y-%m-%d")
            return "date"
        except ValueError:
            try:
                datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")
                return "timestamp"
            except ValueError:
                return None

    def validate_object(text):
        try:
            float(text)
            return "float"
        except ValueError:
            return validate_date(text)

    column_type = []
    for column in dataframe.columns:
        data = dataframe[column].dropna().reset_index(drop=True)
        if data.size == 0:
            column_type.append("varchar")
        elif isinstance(data[0], datetime):
            column_type.append("timestamp")
        elif isinstance(data[0], date):
            column_type.append("date")
        elif str(data.dtype).startswith("object"):
            date_types = [validate_object(i) for i in data.tolist()]
            if sum([not i for i in date_types]) == 0:
                if set(["float"]) == set(date_types):
                    column_type.append("float")
                elif set(["timestamp"]) == set(date_types):
                    column_type.append("timestamp")
                elif set(["date"]) == set(date_types):
                    column_type.append("date")
                else:
                    column_type.append("varchar")
            else:
                column_type.append("varchar")
        elif str(data.dtype).startswith("int"):
            column_type.append("int")
        elif str(data.dtype).startswith("float"):
            column_type.append("float")
    return OrderedDict(zip(list(dataframe.columns), column_type))


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
            sys.stdout.write(
                "\rTransfering [{0}] {1:.2f}%".format("#" * int(percentage / 10), percentage)
            )
            sys.stdout.flush()
