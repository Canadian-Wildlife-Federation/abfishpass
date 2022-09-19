# cwf-alberta
Store scripts related to habitat modelling in Alberta / CWF

# Overview

This project contains a set of scripts to load source Alberta data into a PostgreSQL database and compute elevation values for the stream network.

# Copyright

Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks

# License

Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0

# Software Requirements
* Python (tested with version 3.9.5)
    * Modules: shapely, psycopg2, tifffile, requests
    
    
* GDAL/OGR (comes installed with QGIS or can install standalone)


* PostgreSQL/PostGIS database



# Configuration
All configuration is setup in the config.ini file. Before running any scripts you should ensure the information in this file is correct. 

All of the scripts allow for a custom configuration file to be specified by providing it as the -c argument to the program. If not supplied the default config.ini file will be used. For example:

> prompt> create_db.py -c custom_config.ini


# Processing

Data Processing takes part in three steps: load raw data, processes each watershed, compute summary statistics.


## 1 - Loading RAW Data

The first step is to populate the database with the required data. These load scripts are specific to the data provided for Alberta. Different source data will require modifications to these scripts.

**Scripts**
* load_alberta/create_db.py -> this script creates all the necessary database tables
* load_alberta/load_alberta.py -> this script uses OGR to load the provided alberta data from the gdb file into the PostgreSQL database.

### 1.1 - Configuring Fish Species Model Parameters

As a part of the loading scripts a fish species table is created which contains the fish species of interest for modelling and various modelling parameters. Before processing the watershed these parameters should be reviewed and configured as necessary. 
Note: Currently there is no velocity or channel confinement data. These parameters are placeholders for when this data is added. 


## 2 - Watershed Processing

Processing is completed by watershed id, each watershed is processed into a separate schema in the database. The watershed configuration must be specified in the ini file and the configuration to be used provided to the script (see below).

Currently processing includes:
* Preprocessing step which loads all the streams from the raw datastore into the working schema
* Loading barriers from the CABD barrier database
* Computing Modelled Crossings
* Computing Mainstems  
* Computing an elevation values for all stream segments
* Computing a smoothed elevation value for all stream segments
* Compute gradient for each stream vertex based on vertex elevation and elevation 100m upstream.
* Break stream segments at required locations
* Reassign raw elevation, smoothed elevation to stream segments
* Compute segment gradient based on start, end elevation and length
* Load and snap fish stocking and observation data to stream network
* Compute upstream/downstream statistics for stream network, including number of barriers, fish stocking species and fish survey species
* Compute accessibility models based on stream gradient and barriers
* Compute habitat models
* Compute upstream/downstream statistics for modelled crossings

**Main Script**

process_watershed.py -c config.ini [watershedid]

The watershedid field must be specified as a section header in the config.ini file. The section must describe the watershed processing details for example:

[17010301]  
#Berland: 17010301  
watershed_id = 17010301  
output_schema = ws17010301  
fish_observation_data = C:\temp\fishobservationdata.zip  

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution.
* A raw streams table with id (uuid), name (varchar), strhaler order (integer), watershed_id (varchar), and geometry (linestring) fields. The scripts assume this data is in an equal length projection so the st_length2d(geometry) function returns the length in metres. 

**Output**

* A new schema with a streams table, barrier, modelled crossings and other output tables.
**ALL EXISTING DATA IN THE OUTPUT TABLES WILL BE DELETED**


## 3 - Compute Summary Statistics

Summary statistic are computed across multiple watersheds (specific in the config.ini).  

**Main Script**

compute_watershed_stats.py

**Input Requirements**

* This scripts reads fields from the processed watershed data in the various schemas.   

**Output**

* A new table hydro.habitat_stats that contains various watershed statistics


 
---
#  Individual Processing Scripts

These scripts are the individual processing scripts that are used for the watershed processing steps.

---
#### 1 - PreProcessing

This script creates required database schemas, and loads stream data for the watershed into a working table in this schema.

**Script**

preprocess_watershed.py

**Input Requirements**

* Raw stream network dataset loaded


**Output**

* A database schema named after the watershed id
* A streams table in this schema populated with all streams from the raw dataset

---
#### 2 - Loading Barriers
This script loads dam barriers from the CABD API.
By default, the script uses the nhn_watershed_id for the subject watershed(s) to retrieve features from the API.

**Script**

load_and_snap_barriers_cabd.py

**Input Requirements**

* Access to the CABD API
* Streams table populated from the preprocessing step 

**Output**

* A new barrier table populated with dam barriers from the CABD API
* The barrier table has two geometry fields - the raw field and a snapped field (the geometry snapped to the stream network). The maximum snapping distance is specified in the configuration file.

---
#### 3 - Compute Modelled Crossings
This script computes modelled crossings defined as locations where rail, road, or trails cross stream networks (based on feature geometries). Due to mapping errors these crossing may not actually exist on the ground.


**Script**

load_modelled_crossings.py

**Input Requirements**

* Streams table populated from the preprocessing step
* Rail, rail, and trail data loaded from the load_alberta data scripts 

**Output**

* A new modelled crossings table with a reference to the stream edge the crossing crosses. 
* Modelled crossings with strahler_order >= 6 are classified as sub_type of bridge and a passability status of PASSABLE
* Updated barriers table that now includes modelled crossing that occur on streams with strahler order < 6
 
---
#### 4 - Mainstems

Computes mainstems based on names of streams and longest upstream length.

**Script**

compute_mainstems.py

**Input Requirements**

* Streams table

**Output**

* A new field, mainstem_id, downstream_route_measure and upstream_route_measure, added to the input table. At this point the measure fields are calculated in m


---
#### 5 - Assign Raw Z Value

Drapes a stream network over provided DEMs and computes a rawz value for each vertex in the stream network.

**Script**

assign_raw_z.py

**Input Requirements**

* Directory of tif images representing DEM files. All files should have the same projection and resolution.
* Streams table populated from the preprocessing step

**Output**

* A geometry_raw3d field added to the stream table that represents the 3d geometry for the segment

---
#### 6 - Compute Smoothed Z Value

Takes a set of stream edges with raw z values and smoothes them so that the streams are always flowing down hill.

**Script**

smooth_z.py

**Input Requirements**

* Streams table with id and geometry_raws3d fields (output from the raw z processing)

**Output**

* A new field, geometry_smoothed3d, added to the input table

---
#### 7 - Compute Vertex Gradients

For every stream vertex, this scripts takes the elevation at that point and the elevation along the mainstem at a point 100m upstream and computes the gradient based on those two elevations 

**Script**

compute_vertex_gradient.py

**Input Requirements**

* Streams table with smoothed elevation values

**Output**

* A new table (vertex_gradients) with a single point for every vertex with a gradient calculated. This table includes both the vertex geometry, upstream geometry and elevation values at both those locations

---
#### 8 - Break Streams

This script breaks the stream network at "barriers" and recomputes necessary attributes. 

For this script a barrier is considered to be: a cabd barrier (dam, waterfall), all modelled crossings, and the most downstream vertices with a gradient greater than minimum value specified in the fish_species table for the accessasbility_gradient field in a collection of vertices with gradient values larger than this value.

For example if stream vertcies has these gradient classes:

x = gradient > 0.35

o = gradient < 0.35


x-----x------o------o------x------x-------x-------o---->

1-----2------3------4------5------6-------7-------8---->


Then the stream edge would be split at vertices 2 and 7.

**Script**

break_streams_at_barriers.py

**Input Requirements**

* Streams table smoothed elevation values

**Output**

* a break_points table that lists all the locations where the streams were broken
* updated streams table with mainstem route measures recomputed (in km this time)
* updated modelled crossings table (stream_id is replaces with a stream_id_up and stream_id_down referencing the upstream and downstream edges linked to the point)
  
---
#### 9 - ReAssign Raw Z Value
We recompute z values again based on the raw data so any added vertices and be computed based on the raw data and not interpolated points.

---
#### 10 - ReCompute Smoothed Z Value

---
#### 11 - Compute Segment Gradient

Compute a segment gradient based on the smoothed elevation for the most upstream coordinate, most downstreamm coordinate, and the length of the stream segment

**Script**

compute_segment_gradient.py

**Input Requirements**

* streams table smoothed elevation values

**Output**

* addition of segment_elevation to streams table


---
#### 12 - Load and snap fish observations

Loads fish observation data provided and snaps it to the stream network. 

**Script**

load_and_snap_fishobservations.py

**Input Requirements**

* fish observation data
* stream network

**Output**

* addition of three tables: fish_aquatic_habitat, fish_stocking, and fish_survey

---
#### 13 - Compute upstream and downstream barrier and fish species information.

Computes a number of statistics for each stream segment:
* number of upstream and downstream barriers
* the identifiers of the upstream and downstream barriers
* the fish species stocked (on the stream)
* the fish species which are stocked upstream and downstream 
* the fish species surveyed (on the stream)
* the fish species which were surveyed upstream and downstream

**Script**

compute_updown_barriers_fish.py

**Input Requirements**

* fish observation data
* stream network
* barriers table

**Output**
* addition of statistic fields to stream network table

---
#### 14 - Compute gradient accessibility models

Computes an accessibility value for each fish species for each stream segment based on:
* segment gradient
* maximum accessibility gradient (specified in the fish_species table)
* barrier location
* fish survey and stocking information 

Segments are classified as:
* ACCESSIBLE - when all gradients downstream are less than maximum amount and there are no barriers downstream OR there is fish stocking or fish survey points upstream (for the given species)
* POTENTIALLY ACCESSIBLE - when all gradients downstream are less than the maximum amount but there is a barrier downstream
* NOT ACCESSIBLE - when any downstream gradient is greater than the maximum value

Barriers include:
* CABD loaded barriers (dams, waterfalls)
* modelled crossing on streams with strahler order < 6

**Script**

compute_gradient_accessibility.py

**Input Requirements**

* stream network
* barriers table

**Output**

* addition of an accessibility field to stream network table for each fish species 

---
#### 15 - Compute habitat models

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


---
#### 16 - Compute modelled crossing statistics 

Computes a collection of modelled crossing statistics for each species and habitat model including:
 * total accessible upstream length - total length of streams that are accessible upstream of this point  
 * total upstream length - total upstream length with habitat model = true
 * functional upstream area - computed by walking up the stream network summing up length of stream segments with habitat model = true, stopping at the first barrier encountered (upstream)

 **Script**

compute_modelled_crossings_upstream_values.py

**Input Requirements**
* stream network
* modelled crossing
* barriers

**Output**
* addition of an statistic fields to to the modelled crossings table



# Algorithms 
## Draping Algorithm

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





## Smoothing Algorithm

The smoothing process ensures streams always flow down hill.


Notes:
*This algorithm does not contain any spike detection, so if there is an error in the DEM that causes a significant spike in the stream network this will significantly affect the results.
* Nodes and vertices with no elevation values (NODATA), are ignored in the computation of the min/max values.


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



## Mainstem Algorithm

Mainstems are computed by starting at the sink node and walking up the network. At any confluence the mainsteam is push up the edge that:
1) has the same stream name as the current edge
2) if no edges have the same name then any named edge; if there are multiple named edges it picks the edge with the longest path to a headwater
3) if no named edges; then it  picks the edge with the longest path to a headwater.


---
# Configuration File

[OGR]  
ogr = location of ogr2ogr  executable  
gdalinfo = location of gdalinfo executable  
gdalsrsinfo = location of gdalsrsinfo executable   
proj = *optional* location of proj library  
  
[DATABASE]  
host = database host  
port = database post  
name = database name  
user = database username  
password = database password  
data_schema = name of main schema for holding raw stream data  
stream_table = names of streams table  
fish_species_table = name of fish species table  
working_srid = the srid (3400) of the stream data - these scripts use the function st_length to compute stream length so the raw data should be in a meters based projection (or reprojected before used)  
aquatic_habitat_table = table name for fish aquatic habitat data  
fish_stocking_table = table name for fish stocking data  
fish_survey_table = table name for fish survey data  
  
[CABD_DATABASE] - the barriers database for loading barrier data  
host = CABD host name  
port = CABD port  
name = CABD database name  
user = CABD username  
password = CABD password  
buffer = this is the buffer distance to grab features - the units are in the working_srid so if its meters 200 is reasonable, if it's degrees something like 0.001 is reasonable  
snap_distance = distance (in working srid units) for snapping point features #to the stream network (fish observation data, barrier data etc)  
  
[CREATE_LOAD_SCRIPT]  
raw_data = raw alberta data  
road_table = road table name  
rail_table = rail table name  
trail_table = trail table name  
  
[PROCESSING]  
stream_table = stream table name 

[WATERSHEDID 1] -> there will be one section for each watershed with a unique section name  
watershed_id = watershed id to process  
output_schema = output schema name  
fish_observation_data = zip file containing fish observation data  

[WATERSHEDID 2] -> there will be one section for each watershed with a unique section name  
watershed_id = watershed id to process  
output_schema = output schema name  
fish_observation_data = zip file containing fish observation data  
  
[ELEVATION_PROCESSING]  
dem_directory = directory containing dem   
3dgeometry_field = field name (in streams table) for geometry that stores raw elevation data  
smoothedgeometry_field = field name (in streams table)  for geometry that stores smoothed elevation data  
  
[MAINSTEM_PROCESSING]  
mainstem_id = name of mainstem id field (in streams table)  
downstream_route_measure = name of downstream route measure field  
upstream_route_measure =name  upstream route measure field  
  
[GRADIENT_PROCESSING]  
vertex_gradient_table = table for storing vertex gradient values   
segment_gradient_field = name of segment gradient field (in streams table)  
max_downstream_gradient_field = name of field for storing the maximum downstream segment gradient (in streams table)  
  
[BARRIER_PROCESSING]  
barrier_table = table for storing barriers  
  
[MODELLED_CROSSINGS]  
modelled_crossings_table = table for storing modelled crossings  
strahler_order_barrier_limit = all crossings on streams with strahler order less than this will be considered barriers and treated similar to dams/waterfalls for habitat modelling  

[HABITAT_STATS]
stats_table = this table will be created in the [DATABASE].data_schema schema and contain watershed statistics

watershed_data_schemas=ws17010302,ws17010301 #this is the list of processing schemas to include in the stats the schemas must exist and data must be processed
