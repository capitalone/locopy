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
