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

from setuptools import setup


exec(open('locopy/_version.py').read())

setup(name='locopy',
      version=__version__,
      description='Loading/Unloading to Amazon Redshift using Python',
      url='https://github.com/capitalone/Data-Load-and-Copy-using-Python',
      author='Faisal Dosani',
      author_email='faisal.dosani@capitalone.com',
      license='Apache Software License',
      packages=['locopy'],
      install_requires=[
          'boto3==1.7.21',
          'PyYAML==3.12',
      ],
      extras_require={
          'psycopg2': ['psycopg2-binary==2.7.4'],
          'pg8000': ['pg8000==1.12.1']},
      zip_safe=False)
