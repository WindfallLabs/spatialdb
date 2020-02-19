Changes
=======

Milestones required to bump version to 0.1.0
--------------------------------------------

**GOAL:** A one-time, feature-complete release for Python2.7 (anything after 0.1.0 will be Python3)


SpatialDB:

* [ ] Instructions for installing GDAL, fiona, shapely, geopandas, etc. etc.
    * Compiling sqlite with RTree
    * Compiling SpatiaLite
    * Can we do all this in a Docker container?
* [ ] Seemless I/O of ESRI-compatible, and really ugly shapefiles
    * .prj files
    * Unicode encoding tests
* [ ] ``CloneTable``, ``DropGeoTable``, and ``reproject`` methods
* [ ] Variable names are standardized between (geo)pandas, SpatiaLite, SQLite, etc.  
* [ ] A good amount of examples



Version 0.0.2 (January, 2020)
-----------------------------

* Added ``get_sr_from_web`` to get srs data from spatialreference.org_
* Added ``SpatiaLiteBlobElement`` to handle Blob geometry decoding
* Added ``SpatiaLiteDB`` subclass of ``db2.SQLiteDB``
    * Added ``SpatiaLiteDB.geometries`` property to quickly get spatial table information
    * Added ``SpatiaLiteDB.load_geodataframe`` to CREATE and INSERT data into the database from ``geopandas.GeoDataFrame`` objects
        * Checks and handles for single geometry type integrity
    * Altered ``SpatiaLiteDB.sql()`` to return a valid GeoDataFrame
        * Automatically converts SpatiaLite Blob geometries to shapely
        * Sets ``GeoDataFrame.crs`` attribute 


.. _spatialreference.org: https://www.spatialreference.org
