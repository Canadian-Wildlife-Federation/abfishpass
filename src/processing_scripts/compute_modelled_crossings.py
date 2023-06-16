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
# This script computes modelled crossings (stream intersections
# with road/trail network) and attributes that can be derived
# from the stream network
#
import appconfig
from appconfig import dataSchema

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbWatershedId = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']

roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
railTable = appconfig.config['CREATE_LOAD_SCRIPT']['rail_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
    
dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

with appconfig.connectdb() as conn:

    query = f"""
    SELECT code
    FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        specCodes = cursor.fetchall()

def tableExists(connection):

    query = f"""
    SELECT EXISTS(SELECT 1 FROM information_schema.tables 
    WHERE table_catalog='{appconfig.dbName}' AND 
        table_schema='{dbTargetSchema}' AND 
        table_name='{dbModelledCrossingsTable}');
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        result = result[0]

    return result

def createTable(connection):

    result = tableExists(connection)

    if result:

        # create an archive table so we can keep modelled_id stable
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbModelledCrossingsTable}_archive;
            CREATE TABLE {dbTargetSchema}.{dbModelledCrossingsTable}_archive 
            AS SELECT * FROM {dbTargetSchema}.{dbModelledCrossingsTable};
            
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbModelledCrossingsTable};
            
            CREATE TABLE {dbTargetSchema}.{dbModelledCrossingsTable} (
                modelled_id uuid default gen_random_uuid(),
                stream_name varchar,
                strahler_order integer,
                stream_id uuid, 
                transport_feature_name varchar,
              
                crossing_status varchar,
                crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'TRAIL')),
                crossing_type varchar,
                crossing_subtype varchar,
                
                geometry geometry(Point, {appconfig.dataSrid}),
                
                primary key (modelled_id)
            );
            
        """
    
        with connection.cursor() as cursor:
            cursor.execute(query)
        
        # add species-specific passability fields
        for species in specCodes:
            code = species[0]

            colname = "passability_status_" + code
            
            query = f"""
                alter table {dbTargetSchema}.{dbModelledCrossingsTable} 
                add column if not exists {colname} numeric;
            """

            with connection.cursor() as cursor:
                cursor.execute(query)
    
    else:
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbModelledCrossingsTable};
            
            CREATE TABLE {dbTargetSchema}.{dbModelledCrossingsTable} (
                modelled_id uuid default gen_random_uuid(),
                stream_name varchar,
                strahler_order integer,
                stream_id uuid, 
                transport_feature_name varchar,
                
                crossing_status varchar,
                crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'TRAIL')),
                crossing_type varchar,
                crossing_subtype varchar,
                
                geometry geometry(Point, {appconfig.dataSrid}),
                
                primary key (modelled_id)
            );
            
        """
    
        with connection.cursor() as cursor:
            cursor.execute(query)

        # add species-specific passability fields 
        for species in specCodes:
            code = species[0]

            colname = "passability_status_" + code
            
            query = f"""
                alter table {dbTargetSchema}.{dbModelledCrossingsTable} 
                add column if not exists {colname} numeric;
            """

            with connection.cursor() as cursor:
                cursor.execute(query)

def computeCrossings(connection):
        
    query = f"""
        --roads
        INSERT INTO {dbTargetSchema}.{dbModelledCrossingsTable} 
            (stream_name, strahler_order, stream_id, transport_feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b."name" as transport_feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{roadTable} b
                where st_intersects(a.geometry, b.geometry)
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, transport_feature_name, 'ROAD', pnt 
            from points
        );
        
        
        --trail
        INSERT INTO {dbTargetSchema}.{dbModelledCrossingsTable} 
            (stream_name, strahler_order, stream_id, transport_feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b."name" as transport_feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{trailTable} b
                where st_intersects(a.geometry, b.geometry)
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, transport_feature_name, 'TRAIL', pnt 
            from points
        );
        
        --delete any duplicate points within a very narrow tolerance
        --duplicate points may result from transport features being broken on streams
        DELETE FROM {dbTargetSchema}.{dbModelledCrossingsTable} p1
        WHERE EXISTS (SELECT FROM {dbTargetSchema}.{dbModelledCrossingsTable} p2
            WHERE p1.modelled_id > p2.modelled_id
            AND ST_DWithin(p1.geometry,p2.geometry,0.01));

    """
    # print(query)
    with connection.cursor() as cursor:
        cursor.execute(query)

def matchArchive(connection):

    query = f"""
        WITH matched AS (
            SELECT
            a.modelled_id,
            nn.modelled_id as archive_id,
            nn.dist
            FROM {dbTargetSchema}.{dbModelledCrossingsTable} a
            CROSS JOIN LATERAL
            (SELECT
            modelled_id,
            ST_Distance(a.geometry, b.geometry) as dist
            FROM {dbTargetSchema}.{dbModelledCrossingsTable}_archive b
            ORDER BY a.geometry <-> b.geometry
            LIMIT 1) as nn
            WHERE nn.dist < 10
        )

        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable} a
            SET modelled_id = m.archive_id
            FROM matched m
            WHERE m.modelled_id = a.modelled_id;

        DROP TABLE {dbTargetSchema}.{dbModelledCrossingsTable}_archive;

    """
    with connection.cursor() as cursor:
        cursor.execute(query)

def computeAttributes(connection):
    
    #assign all modelled crossings a crossing_status and passability_status
    
    query = f"""
        UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
        SET crossing_status = 'MODELLED';
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    
    for species in specCodes:
        code = species[0]

        colname = "passability_status_" + code
            
        query = f"""
            UPDATE {dbTargetSchema}.{dbModelledCrossingsTable}
            SET {colname} = 1 WHERE {colname} IS NULL;
        """

        with connection.cursor() as cursor:
            cursor.execute(query)

def loadToBarriers(connection):

    newCols = []

    for species in specCodes:
        code = species[0]

        col = "passability_status_" + code
        newCols.append(col)
    
    colString = ','.join(newCols)

    query = f"""
        DELETE FROM {dbTargetSchema}.{dbBarrierTable} WHERE type = 'stream_crossing';
        
        INSERT INTO {dbTargetSchema}.{dbBarrierTable}(
            modelled_id, snapped_point,
            type, {colString},
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype
        )
        SELECT 
            modelled_id, geometry,
            'stream_crossing', {colString},
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype
        FROM {dbTargetSchema}.{dbModelledCrossingsTable};

        UPDATE {dbTargetSchema}.{dbBarrierTable} SET wshed_name = '{dbWatershedId}';
        
        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def main():                        
    #--- main program ---    
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Modelled Crossings")

        tableExists(conn)

        result = tableExists(conn)

        if result:

            print("  creating tables")
            createTable(conn)
            
            print("  computing modelled crossings")
            computeCrossings(conn)

            print("  matching to archived crossings")
            matchArchive(conn)
            
            conn.commit()

            print("  calculating modelled crossing attributes")
            computeAttributes(conn)

            print("  loading to barriers table")
            loadToBarriers(conn)
            
            conn.commit()
        
        else:
            print("  creating tables")
            createTable(conn)
            
            print("  computing modelled crossings")
            computeCrossings(conn)

            print("  calculating modelled crossing attributes")
            computeAttributes(conn)

            print("  loading to barriers table")
            loadToBarriers(conn)
            
            conn.commit()
                
    print("done")

if __name__ == "__main__":
    main() 