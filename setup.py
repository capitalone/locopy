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

with open(os.path.join(CURR_DIR, "requirements.txt"), encoding="utf-8") as file_open:
    INSTALL_REQUIRES = file_open.read().split("\n")

exec(open("locopy/_version.py").read())


# No versioning on extras for dev, always grab the latest
EXTRAS_REQUIRE = {
    "psycopg2": ["psycopg2-binary>=2.7.7"],
    "pg8000": ["pg8000>=1.13.1"],
    "snowflake": ["snowflake-connector-python[pandas]>=2.1.2"],
    "docs": ["sphinx", "sphinx_rtd_theme"],
    "tests": [
        "hypothesis",
        "pytest",
        "pytest-cov",
    ],
    "qa": [
        "pre-commit",
        "black",
        "isort",
    ],
    "build": ["twine", "wheel"],
    "edgetest": ["edgetest", "edgetest-conda"],
}

EXTRAS_REQUIRE["dev"] = (
    EXTRAS_REQUIRE["tests"]
    + EXTRAS_REQUIRE["docs"]
    + EXTRAS_REQUIRE["qa"]
    + EXTRAS_REQUIRE["build"]
)


setup(
    name="locopy",
    version=__version__,
    description="Loading/Unloading to Amazon Redshift using Python",
    long_description=LONG_DESCRIPTION,
    url="https://github.com/capitalone/locopy",
    author="Faisal Dosani",
    author_email="faisal.dosani@capitalone.com",
    license="Apache Software License",
    packages=["locopy"],
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    zip_safe=False,
)
