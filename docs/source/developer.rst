Developer Instructions
======================

Guidance for developers.

Pre-Commit Hooks
----------------

We use the excellent `pre-commit <https://pre-commit.com/>`_ to run the excellent
`black <https://github.com/ambv/black>`_ on all changes before commits.  ``pre-commit`` is included
in the black requirements below, and you'll have to run ``pre-commit install`` once per environment
before committing changes, or else manually install ``black`` and run it.  If you have ``pre-commit``
installed, trying to commit a change will first run black against any changed Python files, and force
you to add/commit any changes.

The reason behind running black as a pre-commit hook is to let a machine make style decisions, based
on the collective wisdom of the Python community.  The only change made from the default black setup
is to allow lines up to 100 characters long.

Generating Documentation
------------------------

You will need to ``pip install`` the test requirements::

    pip install -r requirements-dev.txt

From the root directory type::

    make sphinx

This will automatically regenerate the api documentation using ``sphinx-apidoc``.
The rendered documentation will be stored in the ``/docs`` directory.  The
GitHub pages sits on the ``gh-pages`` branch.

To push the documentation changes to the ``gh-pages`` branch::

    make ghpages && git push origin gh-pages

Note about documentation: The `Numpy and Google style docstrings
<http://sphinx-doc.org/latest/ext/napoleon.html>`_ are activated by default.
Just make sure Sphinx 1.3 or above is installed.


Run unit tests
--------------

Run ``make coverage`` to run all unittests defined in the subfolder
``tests`` with the help of `py.test <http://pytest.org/>`_ and
`pytest-runner <https://pypi.python.org/pypi/pytest-runner>`_.


Integration Tests
-----------------

Run ``make integration`` to run all integration tests (or just run
``make not_integration`` to run only unit and not integration tests).  For the
integration tests to work, you will need:

- A Redshift connection that you can create tables in
- Tokens set up in ``~/.aws/credentials`` with a profile name
- A file ``~/.locopyrc`` which contains credential information use for running
  integration tests. It should look like the following:

.. code-block:: yaml

    host: my.redshift.cluster.com
    port: 5439
    dbname: db
    user: username
    password: password
    profile: MY_AWS_PROFILE


The integration tests run through a number of selects, loads and unloads.  They
try to clean up after themselves, but you may not want to run them without
knowing what they're doing.  Caveat emptor!



Management of Requirements
--------------------------

Requirements of the project should be added to ``requirements.txt``.  Optional
requirements used only for testing are added to ``requirements-dev.txt``. There is a third
requirements file ``requirements-black.txt`` which is used only for code formatting and pre-commit
hooks. Black only works with Python 3.6.0+ and conflicts with the Travis CI build (Python 3.5)



Generating distribution archives (PyPI)
---------------------------------------

After each release the package will need to be uploaded to PyPi. The instructions below are taken
from `packaging.python.org <https://packaging.python.org/tutorials/packaging-projects/#generating-distribution-archives>`_

Update / Install ``setuptools``, ``wheel``, and ``twine``::

    pip install --upgrade setuptools wheel twine

Generate distributions::

    python setup.py sdist bdist_wheel

Under the ``dist`` folder you should have something as follows::

    dist/
    locopy-0.1.0-py3-none-any.whl
    locopy-0.1.0.tar.gz



Finally upload to PyPi::

    # test pypi
    twine upload --repository-url https://test.pypi.org/legacy/ dist/*

    # real pypi
    twine upload dist/*
