SQL Injection
=============

SQL Injection is important topic when dealing when database interactions. The best practices involve
passing parameters into the query as such.

.. code-block:: python

    sql = "SELECT * FROM table WHERE id=(%s);"
    data = (1, )
    cur.execute(sql, data)


Using string concatenation and interpolation is usually a no no when dealing with DB adapters such
as ``psycopg2``. The problem though for ``locopy`` comes from query parameter passing for the non
select type statements (``COPY``, ``UNLOAD``, etc.). These do not play nice with parameter passing.

The ``execute`` method of the ``Database`` class uses parameters, but the ``COPY``/``UNLOAD``
commands unfortunately cannot, so please keep these in mind when building out your application.
It is important to call this out so people are aware of this limitation, and realize using ``locopy``
for a client facing application (think website with input field) might not be the best idea.
Most people will be using ``locopy`` for ETL processing internally where the input usage is
internal vs external, and known to be safer.
