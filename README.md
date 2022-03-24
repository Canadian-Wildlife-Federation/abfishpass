# cwf-alberta
Store scripts related to habitat modelling in Alberta / CWF

# Overview

This project contains a set of scripts to load source Alberta data into a PostgreSQL database and compute elevation values for the stream network.

# Copyright

Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks

# License

Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0

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
* [CABD_DATABASE]:host -> CABD database hostname for loading barriers
* [CABD_DATABASE]:port -> CABD database port for loading barriers
* [CABD_DATABASE]:name -> CABD database name for loading barriers
* [CABD_DATABASE]:user -> CABD database username for loading barriers
* [CABD_DATABASE]:password -> CABD database password for loading barriers
* [CREATE_LOAD_SCRIPT]:raw_data -> link to the source data file
* [CREATE_LOAD_SCRIPT]:road_table -> the database table name to load roads data into
* [CREATE_LOAD_SCRIPT]:rail_table -> the database table name to load rail data into
* [CREATE_LOAD_SCRIPT]:trail_table -> the database table name to load trails data into
* [PROCESSING]:watershed_id -> the watershed id to process
* [PROCESSING]:output_schema -> the database schema to write processing results to
* [PROCESSING]:stream_table -> the main stream tables containing all the streams data for the watershed and computed outputs
* [ELEVATION_PROCESSING]:dem_directory -> location of dem files
* [ELEVATION_PROCESSING]:target_3dgeometry_field -> field name to write raw z geometries to
* [ELEVATION_PROCESSING]:target_smoothedgeometry_field -> field name to write smoothed z geometries to
* [MAINSTEM_PROCESSING]:mainstem_id -> mainstem id field name
* [ELEVATION_PROCESSING]:downstream_route_measure  -> field name to downstream route measure to
* [ELEVATION_PROCESSING]:upstream_route_measure  -> field name to write upstream route measure to
* [GRADIENT_PROCESSING]: vertex_gradient_table -> table to store vertex graident data
* [GRADIENT_PROCESSING]: segment_gradient_field -> field name to write segment graident value to    
* [BARRIER_PROCESSING]: barrier_table -> name of table to load barrier data into

## 1 - Loading Data

The first step is to populate the database with the required data. These load scripts are specific to the data provided for Alberta. Different source data will require modifications to these scripts.

**Scripts**
* load_alberta/create_db.py -> this script creates all the necessary database tables
* load_alberta/load_alberta.py -> this script uses OGR to load the provided alberta data from the gdb file into the PostgreSQL database.



## 2 - Watershed Processing

Processing is completed by watershed id. When processing a watershed all processed data is written to a schema in the database named after the watershed identifier.

Currently processing includes:
* Preprocessing step which loads all the streams from the raw datastore into the working schema
* Loading barriers from the CABD barrier database
* Snapping barriers to stream network and breaking the stream segements at these points
* Computing an elevation values for all stream segements
* Computing a smoothed elevation value for all stream segements
* Computing mainstems based on stream name and upstream length for stream segements
* Computing graident for each stream segements based on the start and end elevation points AND compute gradient for each stream vertex based on vertex elevation and elevation 100m upstream.


**Script**

process_watershed.py

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution.
* A raw streams table with id (uuid), watershed_id (varchar), and geometry (linestring) fields. The scripts assume this data is in an equal length projection so the st_length(geometry) function returns the length in metres. 

**Output**

* A new schema with a streams table, barriers tabels.
**ALL EXISTING DATA IN THE OUTPUT TABLES WILL BE DELETED**
 

### 2 - Watershed Processing Step Scripts

#### 2A - PreProcessing

This script creates required database schemas, and loads stream data for the watershed into a working table in this schema.

**Script**

preprocess_watershed.py

**Input Requirements**

* Raw stream network dataset loaded


**Output**

* A database schema named after the watershed id
* A streams table in this schema populated with all streams from the raw dataset


#### 2B - Loading Barriers
This script loads waterfalls and dam barriers from the CABD database.


**Script**

load_barriers_cabd.py

**Input Requirements**

* Access to the CABD database
* Streams table populated from the preprocessing step 

**Output**

* A new barrier table populated with dam and waterfall barriers from the CABD database


#### 2C - Snapping Barriers and Breaking Streams
This step snaps barriers to the nearset stream network (within 150m), and then breaks the
stream segments into multiple segments at these points.

**Script**

snap_and_break_barriers.py

**Input Requirements**

* Stream table populated from the preprocessing step
* Barriers table  

**Output**

* A snapped_geometry column is added to the barriers table
* Streams table is updated with stream segments broken at snapped barrier points


#### 2D - Assign Raw Z Value
This step drapes a stream network over provided DEMs and computes a rawz value for each vertex in the stream network.

Only a single watershed is processed at a time. 

**Script**

assign_raw_z.py

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution.
* Streams table populated from the preprocessing step

**Output**

* A geometry_raw3d field added to the stream table that represents the 3d geometry for the segment

#### 2E - Compute Smoothed Z Value

This step takes a set of stream edges with raw z values and smoothes them so that the streams are always flowing down hill.

**Script**
smooth_z.py

**Input Requirements**
* Streams table with id and geometry_raws3d fields (output from the raw z processing)

**Output**
* A new field, geometry_smoothed3d, added to the input table

#### 2F - Compute Mainstems

This step computes mainstems based on name and upstream length for the watershed streams.

**Script**
compute_mainstems.py

**Input Requirements**
* Streams table

**Output**
* A new field, mainstem_id, downstream_route_measure and upstream_route_measure, added to the input table

#### 2G - Compute Graident

This step computes two gradients - one for each stream segement based on the smoothed z values at the start and end of the segments. The other is a gradient for each vertex point based on the smoothed value at that point and the values 100m upstream.

**Script**
compute_graident.py

**Input Requirements**
* Streams table with smoothed geometry value

**Output**
* A new table, vertex_graident, that contains the vertex point, upstream point, the elevation at these two points, and gradient computed based on these elevation values and a graident class.

## Algorithms 
### Draping Algorithm

To compute raw elevation, for each vertex:

1. drop the vertex on the DEM and determine which 4 cells are the nearest to the point. In the example below the four nearest cells to V are A, B, C & D.

2. compute a bilinear interpolated value at this point using the values from cells A, B, C, & D.

<pre>
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
</pre>    

Notes: we assume that the elevation values provided in the DEM represent the elevation at the center point of the cell    

### Smoothing Algorithm

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

<pre>
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
        
</pre>        



### Mainstem Algorithm

Mainstems are computed by starting at the sink node and walking up the network. At any confluence the mainsteam is push up the edge that:
1) has the same stream name as the current edge
2) if no edges have the same name then any named edge; if there are multiple named edges it picks the edge with the longest path to a headwater
3) if no named edges; then it  picks the edge with the longest path to a headwater.

