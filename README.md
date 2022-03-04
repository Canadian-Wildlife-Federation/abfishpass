# cwf-alberta
Store scripts related to habitat modelling in Alberta / CWF

# Overview

This project contains a set of scripts to load source Alberta data into a PostgreSQL database and compute elevation values for the stream network.

## Software Requirements
* Python (tested with version 3.9.5)
    * Modules: shapely, psycopg2, tifffile
    
    
* GDAL/ORG (comes installed with QGIS or can install standalone)


* Postgresql/PostGIS database



## Configuration
All configuration is setup in the config.ini file. Before running any scripts you should ensure the information in this file is correct.

All of the scripts allow for a custom configuration file to be specified by providing it as the first argument to the program. If not supplied the default config.ini file will be used. For example:

> prompt> create_db.py custom_config.ini

**Settings**

* [OGR]:org -> location of OGR2OGR program
* [OGR]:gdalinfo -> location of gdalinfo program
* [OGR]:gdalsrsinfo -> location of gdalsrsinfo program
* [OGR]:proj -> OPTIONAL only required if multiple versions of proj are installed 
* [DATABASE]:host -> database hostname
* [DATABASE]:port -> database port
* [DATABASE]:name -> database name
* [DATABASE]:user -> database user
* [DATABASE]:password -> database password
* [DATABASE]:data_schema -> the main data schema to store raw source data
* [DATABASE]:stream_table -> the name of the raw streams source data
* [DATABASE]:working_srid -> the srid of the source data
* [CREATE_LOAD_SCRIPT]:raw_data -> link to the source data file
* [CREATE_LOAD_SCRIPT]:road_table -> the database table name to load roads data into
* [CREATE_LOAD_SCRIPT]:rail_table -> the database table name to load rail data into
* [CREATE_LOAD_SCRIPT]:trail_table -> the database table name to load trails data into
* [PROCESSING]:watershed_id -> the watershed id to process
* [PROCESSING]:output_schema -> the database schema to write processing results to
* [ELEVATION_PROCESSING]:dem_directory -> location of dem files
* [ELEVATION_PROCESSING]:target_table -> output table for elevation processing
* [ELEVATION_PROCESSING]:target_3dgeometry_field -> field name to write raw z geometries to
* [ELEVATION_PROCESSING]:target_smoothedgeometry_field -> field name to write smoothed z geometries to


## 1 - Loading Data

The first step is to populate the database with the required data. These load scripts are specific to the data provided for Alberta. Different source data will require modifications to these scripts.

**Scripts**
* create_db.py -> this script creates all the necessary database tables
* load_alberta.py -> this script uses OGR to load the provided alberta data from the gdb file into the PostgreSQL database.



## 2 - Elevation Processing

The elevation processing is flexible and should work on any datasets (Alberta or other), providing the requirements listed below are met.

### 2A - Assign Raw Z Value
This step drapes a stream network over provided DEMs and computes a rawz value for each vertex in the stream network.

Only a single watershed is processed at a time. 

**Script**

assign_raw_z.py

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution.
* A streams table with id (uuid), watershed_id (varchar), and geometry (linestring) fields.

** Output**

* A new table with id, watershed_id, and geometry_raw3d fields.
**ALL EXISTING DATA IN THE OUTPUT TABLE WILL BE DELETED**

** Relevant Configuration Settings**

All the inputs/outputs are specified in the config.ini file. In particular:

* [PROCESSING]:watershedid -> the watershed to process
* [PROCESSING]:output_schema -> the schema to write results to
* [ELEVATION_PROCESSING]:dem_directory -> location of dem files
* [ELEVATION_PROCESSING]:target_table -> the table to write results to
* [ELEVATION_PROCESSING]:target_3dgeometry_field -> the name of the geometry field to write to

### 2B - Compute Smoothed Z Value

This step takes a set of stream edges with raw z values and smoothes them so that the streams are always flowing down hill.

**Script**
smooth_z.py

**Input Requirements**
* table with id and geometry_raws3d fields (output from the raw z processing)

**Output**
* a new field, geometry_smoothed3d, added to the input table

** Relevant Configuration Settings**

* [PROCESSING]:output_schema -> the schema to write results to
* [ELEVATION_PROCESSING]:target_table -> the table to write results to
* [ELEVATION_PROCESSING]:target_3dgeometry_field -> the name of the geometry with raw z values
* [ELEVATION_PROCESSING]:target_smoothedgeometry_field -> the name of the geometry field to write smoothed results to


## Draping Algorithm

To compute raw elevation, for each vertex:

1. drop the vertex on the DEM and determine which 4 cells are the nearest to the point. In the example below the four nearest cells to V are A, B, C & D.

2. compute a bilinear interpolated value at this point using the values from cells A, B, C, & D.

	 A = (x1, y2, Az)
	 B = (x2, y2, Bz)
	 C = (x1, y1, Cz)
	 D = (x2, y1, Dz)
    V = (x, y, Vz)
    
	 fxy1 = ((x2 - x) / (x2- x1))*Cz + ((x - x1)/(x2 - x1))*Dz
	 fxy2 = ((x2 - x) / (x2- x1))*Az + ((x - x1)/(x2 - x1))*Bz
	 Vz = ((y2 - y) / (y2 - y1))*fxy1 + ((y - y1)/(y2 - y1))*fxy2


    +-------------+-------------+
    |             |             |
    |             |             |
    |      A      |      B      |
    |             |             |
    |             |             |
    +-------------+-------------+
    |          V  |             |
    |             |             |
    |      C      |      D      |
    |             |             |
    |             |             |
    +-------------+-------------+
    

Notes: we assume that the elevation values provided in the DEM represent the elevation at the center point of the cell    

## Smoothing Algorithm

The smoothing process ensures streams always flow down hill.


Notes:
*This algorithm does not contain any spike detection, so if there is an error in the DEM that causes a significant spike in the stream network this will significantly affect the results.
* Nodes and verticies with no elevation values (NODATA), are ignored in the computation of the min/max values.


1. Create a graph of the nodes in the stream network
2. Starting at the sink nodes and walking up the network computing a max_elevation value for each node. This value is the maximum of the node's raw elevation and the downstream node elevation values
3. Starting at the source nodes and walking down the network compute a min_elevation value for each node. This value is the minimum of the node's raw elevation values and the upstream node elevation values.
4. For each node assign a smoothed elevation of the the average of the max_elevation and min_elevation
5. For each edge in the network
  * clip all vertcies elevations so they are no smaller or bigger than the z values at the end nodes
  * compute min/max elevations for each vertex then average the results to get smoothed value 
  
     Node  Elevation   Min  Max   Smoothed
      A       12       12   12    12
      B       10       10   10    10
      C       6        6    7     6.5
      D       7        6    7     6.5      
      F       8        8    8     8      
      G       2        2    2     2
  
    A           B 
     \         /
      \       /
       \     /
        C---+
        |      F
        |     / 
        |    /
        D---+
        |
        |
        |
        F
        
        