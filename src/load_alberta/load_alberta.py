#----------------------------------------------------------------------------------
#
# Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#----------------------------------------------------------------------------------

#
# This script loads gdb files into postgis database, create by the create_db.py script
#
import subprocess
import appconfig
import os


streamTable = appconfig.config['DATABASE']['stream_table']
roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
railTable = appconfig.config['CREATE_LOAD_SCRIPT']['rail_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
huc8Table = appconfig.config['CREATE_LOAD_SCRIPT']['huc8_table']

file = appconfig.config['CREATE_LOAD_SCRIPT']['raw_data']
hucfile = appconfig.config['CREATE_LOAD_SCRIPT']['huc_data']
temptable = appconfig.dataSchema + ".temp"

with appconfig.connectdb() as conn:
    
    print("Loading Streams")
    layer = "Streams"
    datatable = appconfig.dataSchema + "." + streamTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + temptable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)
    
    query = f"""
    INSERT INTO {datatable} (id, waterbody_id, stream_name, feature_type, strahler_order, 
    watershed_id, hydro_code, fish_species, geometry) 
    SELECT uuid_generate_v4(), wb_id, name, feature_type, str_order::integer,
    huc_8, hydro_code, species_pres, st_geometryn(geometry ,1) 
    FROM
    {temptable};
    
    DROP table {temptable};
    """
    #print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    print("Loading Roads")
    layer = "roads"
    datatable = appconfig.dataSchema + "." + roadTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + temptable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)
    
    query = f"""
    INSERT INTO {datatable} (
    id, feature_type, name, highway_number, road_class, geo_source, geo_date, feature_type_source,
    feature_type_date,globalid,update_date,geometry)         
    SELECT uuid_generate_v4(), feature_type, name, hwy_number, 
    road_class, geo_source, geo_date, feature_type_source,
    feature_type_date,globalid,update_date,st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) 
    FROM
    {temptable};

    UPDATE {datatable} SET name = NULL WHERE length(trim(name)) = 0;
    UPDATE {datatable} SET name = trim(name);
    
    DROP table {temptable};
    """
    #print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    
    print("Loading Rail")
    layer = "rail"
    datatable = appconfig.dataSchema + "." + railTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + temptable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)
    
    query = f"""
    INSERT INTO {datatable} (id, feature_type, geo_source, geo_date, 
    feature_type_source, feature_type_date, globalid, update_date, geometry) 
    SELECT uuid_generate_v4(), feature_type, geo_source, geo_date, 
    feature_type_source, feature_type_date, globalid, update_date, st_geometryn(geometry,1) 
    FROM
    {temptable};
    
    DROP table {temptable};
    """
    #print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)        
    conn.commit()

    print("Loading Trails")
    layer = "trails"
    datatable = appconfig.dataSchema + "." + trailTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + temptable + '" -nlt CONVERT_TO_LINEAR -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)
    query = f"""
    INSERT INTO {datatable} (
    id,name,trailid,type,status,season,designation,surface,use_type,accessibility,
    commercial_operator,adopted,land_ownership,average_width,minimum_clearing_width,
    hike,bike,winter_bike,horse,wagon,ohv,two_wheel_motor,side_x_side,vehicle_4x4,snowshoe,
    skateski,classicski,skitour,skijoring,snowvehicle,dogsled,timerestriction1_start,
    timerestriction1_end,timerestriction2_start,timerestriction2_end,timerestriction3_start,
    timerestriction3_end,spatialrestriction1,spatialrestriction2,trail_condition,trail_report_date,
    condition_source,comments,datasource,disposition_number,dateupdate,updateby,datasourcedate,
    link,data_display,level_develop,use_type2,geometry
    ) 
    SELECT uuid_generate_v4(), 
    name,trailid,type,status,season,designation,surface,use_type,accessibility,
    commercial_operator,adopted,land_ownership,average_width,minimumclearing_width,
    hike,bike,winterbike,horse,wagon,ohv,twowheelmotor,sidexside,vehicle4x4,snowshoe,
    skateski,classicski,skitour,skijoring,snowvehicle,dogsled,timerestriction1_start,
    timerestriction1_end,timerestriction2_start,timerestriction2_end,timerestriction3_start,
    timerestriction3_end,spatialrestriction1,spatialrestriction2,trailcondition,trailreportdate,
    conditionsource,comments,datasource,dispositionnum,dateupdate,updateby,datasourcedate,
    link,datadisplay,leveldevelop,usetype,st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) 
    FROM
    {temptable};
    
    --data without geometries
    INSERT INTO {datatable} (
    id,name,trailid,type,status,season,designation,surface,use_type,accessibility,
    commercial_operator,adopted,land_ownership,average_width,minimum_clearing_width,
    hike,bike,winter_bike,horse,wagon,ohv,two_wheel_motor,side_x_side,vehicle_4x4,snowshoe,
    skateski,classicski,skitour,skijoring,snowvehicle,dogsled,timerestriction1_start,
    timerestriction1_end,timerestriction2_start,timerestriction2_end,timerestriction3_start,
    timerestriction3_end,spatialrestriction1,spatialrestriction2,trail_condition,trail_report_date,
    condition_source,comments,datasource,disposition_number,dateupdate,updateby,datasourcedate,
    link,data_display,level_develop,use_type2,geometry
    ) 
    SELECT uuid_generate_v4(), 
    name,trailid,type,status,season,designation,surface,use_type,accessibility,
    commercial_operator,adopted,land_ownership,average_width,minimumclearing_width,
    hike,bike,winterbike,horse,wagon,ohv,twowheelmotor,sidexside,vehicle4x4,snowshoe,
    skateski,classicski,skitour,skijoring,snowvehicle,dogsled,timerestriction1_start,
    timerestriction1_end,timerestriction2_start,timerestriction2_end,timerestriction3_start,
    timerestriction3_end,spatialrestriction1,spatialrestriction2,trailcondition,trailreportdate,
    conditionsource,comments,datasource,dispositionnum,dateupdate,updateby,datasourcedate,
    link,datadisplay,leveldevelop,usetype,st_setsrid('LINESTRING EMPTY'::geometry,{appconfig.dataSrid}) 
    FROM
    {temptable}
    WHERE st_numgeometries(geometry) = 0;
    
    UPDATE {datatable} SET name = NULL WHERE length(trim(name)) = 0;
    UPDATE {datatable} SET name = trim(name);

    DROP table {temptable};

    """
    
    #print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    print("Loading HUC 8 Watershed Boundaries")
    layer = "HydrologicUnitCode8WatershedsOfAlberta"
    datatable = appconfig.dataSchema + "." + huc8Table
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + datatable + '" -nlt CONVERT_TO_LINEAR -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geometry "' + hucfile + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)

print("Loading Alberta dataset complete")
