About spatialdb
===============


.. image:: https://img.shields.io/pypi/v/db2.svg
        :target: https://pypi.python.org/pypi/db2

.. image:: https://img.shields.io/travis/WindfallLabs/db2.svg
        :target: https://travis-ci.org/WindfallLabs/db2

.. image:: https://readthedocs.org/projects/db2/badge/?version=latest
        :target: https://db2.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/WindfallLabs/db2/shield.svg
     :target: https://pyup.io/repos/github/WindfallLabs/db2/
     :alt: Updates


**NOTE** This version (v0.0.1) is not yet ready for public consumption,
please use caution and expect frequent changes.  


**FROM DB2**
My personal interests in developing db2 stems from my needs to work with
spatial databases. I needed some major modifications to db.py to make
SpatiaLite work with ``geopandas``. I hope that this project may help
others with their needs as well.


Features
--------

* SQL execution returns ``pandas`` DataFrames
* Support for multiple database flavors:
   * SQLite
   * PostgreSQL
   * MS SQL Server
   * (others coming soon)


TODO
----

See changes_


.. _Db2: https://wiki.python.org/moin/DB2
.. _db.py: http://blog.yhat.com/posts/introducing-db-py.html
.. _commit: https://github.com/yhat/db.py
.. _changes: changes.html