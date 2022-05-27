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
* [GRADIENT_PROCESSING]: vertex_gradient_table -> table to store vertex gradient data
* [GRADIENT_PROCESSING]: segment_gradient_field -> field name to write segment gradient value to    
* [BARRIER_PROCESSING]: barrier_table -> name of table to load barrier data into

## 1 - Loading Data

The first step is to populate the database with the required data. These load scripts are specific to the data provided for Alberta. Different source data will require modifications to these scripts.

**Scripts**
* load_alberta/create_db.py -> this script creates all the necessary database tables
* load_alberta/load_alberta.py -> this script uses OGR to load the provided alberta data from the gdb file into the PostgreSQL database.

### 1.1 - Configuring Fish Species Model Parameters

As a part of the loading scripts a fish species table is created which contains the fish species of interest for modelling and various modelling parameters. Before processing the watershed these parameters should be reviewed and configured as necessary. 
Note: Currently there is no velocity or channel confinement data. These parameters are placeholders for when this data is added. 


## 2 - Watershed Processing

Processing is completed by watershed id. When processing a watershed all processed data is written to a schema in the database named after the watershed identifier.

Currently processing includes:
* Preprocessing step which loads all the streams from the raw datastore into the working schema
* Loading barriers from the CABD barrier database
* Computing Modelled Crossings
* Computing Mainstems  
* Computing an elevation values for all stream segments
* Computing a smoothed elevation value for all stream segments
* Compute gradient for each stream vertex based on vertex elevation and elevation 100m upstream.
* Break stream segments at "barriers" and "gradient barriers"
* Reassign raw elevation, smoothed elevation to stream segments
* Compute segment gradient based on start, end elevation and length
* Load and snap fish stocking and observation data to stream network
* Compute upstream/downstream statistics for stream network, including number of barriers, fish stocking species and fish survey species
* Compute accessibility models based on stream gradient and barriers
* Compute habitat models
* Compute upstream/downstream statistics for modelled crossings


**Main Script**

process_watershed.py

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution.
* A raw streams table with id (uuid), name (varchar), strhaler order (integer), watershed_id (varchar), and geometry (linestring) fields. The scripts assume this data is in an equal length projection so the st_length2d(geometry) function returns the length in metres. 

**Output**

* A new schema with a streams table, barrier, modelled crossings and other output tables.
**ALL EXISTING DATA IN THE OUTPUT TABLES WILL BE DELETED**
 

### 2 - Individual Processing Scripts

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

load_and_snap_barriers_cabd.py

**Input Requirements**

* Access to the CABD database
* Streams table populated from the preprocessing step 

**Output**

* A new barrier table populated with dam and waterfall barriers from the CABD database
* The barrier table has two geometry fields - the raw field and a snapped field (the geometry snapped to the stream network). The maximum snapping distance is specified in the configuration file.


#### 2C - Compute Modelled Crossings
This script computes modelled crossings defined as locations where rail, road, or trails cross stream networks (based on feature geometries). Due to mapping errors these crossing may not actually exist on the ground.


**Script**

load_modelled_corssings.py

**Input Requirements**

* Streams table populated from the preprocessing step
* Rail, rail, and trail data loaded from the load_alberta data scripts 

**Output**

* A new modelled crossings table with a reference to the stream edge the crossing crosses. 
*M odelled crossings with strahler_order >= 5 are classified as sub_type of bridge and a passability status of PASSABLE
* Updated barriers table that now includes modelled crossing that occur on streams with strahler order < 5
 

#### 2D - Mainstems


Computes mainstems based on names of streams and longest upstream length.

**Script**

compute_mainstems.py

**Input Requirements**

* Streams table

**Output**

* A new field, mainstem_id, downstream_route_measure and upstream_route_measure, added to the input table. At this point the measure fields are calculated in m



#### 2E - Assign Raw Z Value

Drapes a stream network over provided DEMs and computes a rawz value for each vertex in the stream network.

**Script**

assign_raw_z.py

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution.
* Streams table populated from the preprocessing step

**Output**

* A geometry_raw3d field added to the stream table that represents the 3d geometry for the segment

#### 2E - Compute Smoothed Z Value

Takes a set of stream edges with raw z values and smoothes them so that the streams are always flowing down hill.

**Script**

smooth_z.py

**Input Requirements**

* Streams table with id and geometry_raws3d fields (output from the raw z processing)

**Output**

* A new field, geometry_smoothed3d, added to the input table

#### 2F - Compute Vertex Gradients

For every stream vertex, this scripts takes the elevation at that point and the elevation along the mainstem at a point 100m upstream and computes the gradient based on those two elevations 

**Script**

compute_vertex_gradient.py

**Input Requirements**

* Streams table with smoothed elevation values

**Output**

* A new table (vertex_gradients) with a single point for every vertex with a gradient calculated. This table includes both the vertex geometry, upstream geometry and elevation values at both those locations


#### 2G - Break Streams

This script breaks the stream network at barriers and recomputes necessary attributes. 

For this script a barrier is considered to be: a cabd barrier (dam, waterfall), all modelled crossings, and the most downstream verticies with a gradient greater than minimum value specified in the fish_species table for the accessasbility_gradient field in a collection of verticies with gradient values larger than this value.

For example if stream vertcies has these gradient classes:

x = gradient > 0.35

o = gradient < 0.35


x-----x------o------o------x------x-------x-------o---->

1-----2------3------4------5------6-------7-------8---->


Then the stream edge would be split at verticies 2 and 7.

**Script**

break_streams_at_barriers.py

**Input Requirements**

* Streams table smoothed elevation values

**Output**

* a break_points table that lists all the locations where the streams were broken
* updated streams table with mainstem route measures recomputed (in km this time)
* updated modelled crossings table (stream_id is replaces with a stream_id_up and stream_id_down referencing the upstream and downstream edges linked to the point)
  

#### 2H - ReAssign Raw Z Value
We recompute z values again based on the raw data so any added verticies and be computed based on the raw data and not interpolated points.
 
#### 2I - ReCompute Smoothed Z Value


#### 2J - Compute Segment Gradient

Compute a segment gradient based on the smoothed elevation for the most upstream coordinate, most downstreamm coordinate, and the length of the stream segment

**Script**

compute_segment_gradient.py

**Input Requirements**

* streams table smoothed elevation values

**Output**

* addition of segment_elevation to streams table



#### 2K - Load and snap file observations

Loads fish observation data provided and snaps it to the stream network. 

**Script**

load_and_snap_fishobservations.py

**Input Requirements**

* fish observation data
* stream network

**Output**

* addition of three tables: fish_aquatic_habitat, fish_stocking, and fish_survey


#### 2L - Compute upstream and downstream barrier and fish species information.

Computes a number of statistics for each stream segment:
* number of upstream and downstream barriers
* the identifiers of the upstream and downstream barriers
* the fish species which are stocked upstream and downstream 
* the fish species which were surveyed upstream and downstream

**Script**

compute_updown_barriers_fish.py

**Input Requirements**

* fish observation data
* stream network
* barriers table

**Output**
* addition of statistic fields to stream network table


#### 2M - Compute gradient accessibility models

Computes an accessibility value for each fish species for each stream segment based on:
* segment gradient
* maximum accessibility gradient (specified in the fish_species table)
* barrier location

Segments are classified as:
* ACCESSIBILE - when all gradients downstream are less than maximum amount and there are no barriers downstream
* POTENTIAL ACCESSIBLE - when all gradients downstream are less than the maximum amount but there is a barrier downstream
* NOT ACCESSIBLE - when any downstream gradient is greater than the maximum value

Barriers include:
* CABD loaded barriers (dams, waterfalls)
* modelled crossing on streams with strahler order < 5

**Script**

compute_gradient_accessbility.py

**Input Requirements**

* stream network
* barriers table

**Output**

* addition of an accessibility field to stream network table for each fish species 


#### 2N - Compute habitat models

Computes a true/false value for the following habitat models for each stream segment.

* Gradient: stream_gradient ≥ min_gradient AND stream_gradient < max_gradient AND species_accessibility IN (ACCESSIBLE OR POTENTIALLY ACCESSIBLE)
* Discharge (m3/s): stream_discharge ≥ min_discharge AND stream_discharge < max_discharge AND species_accessibility IN (ACCESSIBLE OR POTENTIALLY ACCESSIBLE)
* Channel confinement (ratio of valley width / channel width): always true for now - model to be defined later

min_gradient, max_gradient, min_discharge, max_discharge are parameters defined for each fish species in the fish_species table

**Script**

compute_habitat_models.py


**Input Requirements**

* stream network

**Output**
* addition of an habitat model fields to stream network table for each fish species



#### 2N - Compute modelled crossing statistics 

Computes a collection of modelled crossing statistics for each species and habitat model including:
 * total accessible upstream length - total length of streams that are accessible upstream of this point  
 * total upstream length - total upstream length with habitat model = true
 * functional upstream area - computed by walking up the stream network summing up length of stream segments with habitat model = true, stopping at the first barrier encountered (upstream)

**Input Requirements**
* stream network
* modelled crossing
* barriers

**Output**
* addition of an statistic fields to to the modelled crossings table



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

