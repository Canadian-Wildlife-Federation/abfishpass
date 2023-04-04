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
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
watershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_table']

file = appconfig.config['CREATE_LOAD_SCRIPT']['raw_data']
watershedfile = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_data']
temptable = appconfig.dataSchema + ".temp"

with appconfig.connectdb() as conn:

    print("Loading Watershed Boundaries")
    layer = "PEIWatersheds"
    datatable = appconfig.dataSchema + "." + watershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + datatable + '" -nlt CONVERT_TO_LINEAR -lco GEOMETRY_NAME=geometry "' + watershedfile + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)
    
    print("Loading Streams")
    layer = "stream"
    datatable = appconfig.dataSchema + "." + streamTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + temptable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    print(pycmd)
    subprocess.run(pycmd)
    
    query = f"""
    TRUNCATE TABLE {datatable};

    INSERT INTO {datatable} (
        id,
        stream_name,
        strahler_order,
        ef_type,
        ef_subtype,
        rank,
        length,
        aoi_id,
        from_nexus_id,
        to_nexus_id,
        ecatchment_id,
        graph_id,
        mainstem_id,
        max_uplength,
        hack_order,
        horton_order,
        shreve_order,
        objectid,
        enabled,
        hydroid,
        hydrocode,
        reachcode,
        name,
        lengthkm,
        lengthdown,
        flowdir,
        ftype,
        edgetype,
        shape_leng,
        primary_,
        secondary,
        tertiary,
        label,
        source,
        picture,
        field_date,
        stream_sou,
        comment,
        flipped,
        from_node,
        to_node,
        nextdownid,
        fromelev,
        toelev,
        geometry)

    SELECT 
        id::uuid,
        name,
        strahler_order,
        ef_type,
        ef_subtype,
        rank,
        length,
        aoi_id,
        from_nexus_id::uuid,
        to_nexus_id::uuid,
        ecatchment_id::uuid,
        graph_id,
        mainstem_id::uuid,
        max_uplength,
        hack_order,
        horton_order,
        shreve_order,
        objectid,
        enabled,
        hydroid,
        hydrocode,
        reachcode,
        name,
        lengthkm,
        lengthdown,
        flowdir,
        ftype,
        edgetype,
        shape_leng,
        primary_,
        secondary,
        tertiary,
        label,
        source,
        picture,
        field_date,
        stream_sou,
        comment,
        flipped,
        from_node,
        to_node,
        nextdownid,
        fromelev,
        toelev,
        st_geometryn(geometry,1) 
    FROM
    {temptable};
    
    DROP table {temptable};
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    print("Loading Roads")
    layer = "road"
    datatable = appconfig.dataSchema + "." + roadTable
    wshedtable = appconfig.dataSchema + "." + watershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + temptable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)
    
    query = f"""
    TRUNCATE TABLE {datatable};

    INSERT INTO {datatable}(
        id,
        name,
        geometry)       
    SELECT
        gen_random_uuid(),
        t1.name,
        CASE
            WHEN ST_WITHIN(t1.geometry,t2.geometry)
            THEN t1.geometry
            ELSE ST_Intersection(t1.geometry, t2.geometry)
            END AS geometry 
    FROM
    {temptable} t1
    JOIN {wshedtable} t2 ON ST_Intersects(t1.geometry, t2.geometry);

    UPDATE {datatable} SET name = NULL WHERE name = 'Placemark';
    UPDATE {datatable} SET name = NULL WHERE length(trim(name)) = 0;
    UPDATE {datatable} SET name = trim(name);
    
    DROP table {temptable};
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    
    print("Loading Trails")
    layer = "trail"
    datatable = appconfig.dataSchema + "." + trailTable
    wshedtable = appconfig.dataSchema + "." + watershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + temptable + '" -nlt CONVERT_TO_LINEAR -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    #print(pycmd)
    subprocess.run(pycmd)
    query = f"""
    TRUNCATE TABLE {datatable};

    INSERT INTO {datatable} (
        id,
        name,
        status,
        zone,
        geometry
    ) 
    SELECT
        gen_random_uuid(),
        t1.name,
        status,
        zone,
        CASE
            WHEN ST_WITHIN(t1.geometry,t2.geometry)
            THEN t1.geometry
            ELSE ST_Intersection(t1.geometry, t2.geometry)
            END AS geometry
    FROM
    {temptable} t1
    JOIN {wshedtable} t2 ON ST_Intersects(t1.geometry, t2.geometry);

    DROP table {temptable};

    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

print("Loading PEI dataset complete")