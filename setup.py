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

import os
from setuptools import setup


CURR_DIR = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(CURR_DIR, "README.rst"), encoding="utf-8") as file_open:
    LONG_DESCRIPTION = file_open.read()

exec(open("locopy/_version.py").read())

setup(
    name="locopy",
    version=__version__,
    description="Loading/Unloading to Amazon Redshift using Python",
    long_description=LONG_DESCRIPTION,
    url="https://github.com/capitalone/Data-Load-and-Copy-using-Python",
    author="Faisal Dosani",
    author_email="faisal.dosani@capitalone.com",
    license="Apache Software License",
    packages=["locopy"],
    install_requires=["boto3==1.9.92", "PyYAML==4.2b4", "pandas>=0.19.0"],
    extras_require={
        "psycopg2": ["psycopg2==2.7.7"],
        "pg8000": ["pg8000==1.13.1"],
        "snowflake": ["snowflake-connector-python==1.7.0"],
    },
    zip_safe=False,
)
