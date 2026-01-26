Developer Instructions
======================

Guidance for developers.

Pre-Commit Hooks
----------------

We use the excellent `pre-commit <https://pre-commit.com/>`_ to run several hooks on all changes before commits.
``pre-commit`` is included in the ``dev`` extra installs. You'll have to run ``pre-commit install`` once per environment
before committing changes.

The reason behind running ruff and others as a pre-commit hook is to let a machine make style decisions, based
on the collective wisdom of the Python community.

Generating Documentation
------------------------

You will need to ``pip install`` the ``dev`` requirements::

    pip install -e .[dev]

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
- A file ``~/.locopyrc`` (``~/.locpy-sfrc`` for snowflake) which contains credential information
  use for running integration tests. It should look like the following

Redshift

.. code-block:: yaml

    host: my.redshift.cluster.com
    port: 5439
    database: db
    user: username
    password: password
    profile: MY_AWS_PROFILE


Snowflake

.. code-block:: yaml

    account: prod
    host: mycluster.url.com
    warehouse: warehouse
    database: db
    schema: schema
    user: username
    password: password
    profile: MY_AWS_PROFILE


The integration tests run through a number of selects, loads and unloads.  They
try to clean up after themselves, but you may not want to run them without
knowing what they're doing.  Caveat emptor!



Management of Requirements
--------------------------

Requirements of the project should be added to ``dependencies`` within our ``pyproject.toml``.  Optional requirements used only for testing,
documentation, or code quality are added to the ``project.optional-dependencies`` of our ``pyproject.toml``.

edgetest
--------

edgetest is a utility to help keep requirements up to date and ensure a subset of testing requirements still work.
More on edgetest `here <https://github.com/capitalone/edgetest>`_.

The ``pyproject.toml`` has configuration details on how to run edgetest. This process can be automated via GitHub Actions.

In order to execute edgetest locally you can run the following after install ``edgetest``:

.. code-block:: bash

    edgetest -c pyproject.toml --export

This should return output like the following and also update our ``dependencies`` within the ``pyproject.toml``:

.. code-block:: bash

    =============  ===============  ===================  =================
    Environment    Passing tests    Upgraded packages    Package version
    =============  ===============  ===================  =================
    core           True             boto3                1.21.7
    core           True             pandas               1.3.5
    core           True             PyYAML               6.0
    =============  ===============  ===================  =================


Release Guide
-------------

Starting in 2026 we've modified our git workflow.

``main`` will be the core branch where all features will get merged into and
production ready code will also exist. When a release is ready to be made releases/tags will be use
to point to the commit where the release should be made from.

In the past we had used ``dev``, in conjunction with ``main``, but there were some issues
with diverging branches and new rulesets which make the ``main`` only branching strategy
easier to manage.

Each release on ``main`` should be tagged properly to denote a "version" that will have the
corresponding artifact on PyPI for users to ``pip install``.


``gh-pages`` is where official documentation will go. After each release you should
build the docs and push the HTML to the pages branch. When first setting up the
repo you want to make sure your gh-pages is a orphaned branch since it is
disconnected and independent from the code: ``git checkout --orphan gh-pages``.

To build the documentation, run ``make sphinx``. To push, run ``make ghpages``.



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
