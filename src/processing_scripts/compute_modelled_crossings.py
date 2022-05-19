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
# with road/rail/trail network) and attributes that can be derived
# from the stream network
#
import appconfig

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbCrossingsTable = appconfig.config['MODELLED_CROSSINGS']['modelled_crossings_table']


roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table'];
railTable = appconfig.config['CREATE_LOAD_SCRIPT']['rail_table'];
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table'];
    
def createTable(connection):
    #note: we will add to this table for
    #each species as the values
    #are computed for those species
    
    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbCrossingsTable};
        
        CREATE TABLE {dbTargetSchema}.{dbCrossingsTable} (
            id uuid default uuid_generate_v4(),
            disp_num varchar,
            stream_name varchar,
            strahler_order integer,
            stream_id uuid, 
            stream_measure numeric,
            wshed_name varchar,
            wshed_priority varchar,
            feature_name varchar,
            ownership_type varchar,
            
            species_upstr varchar[],
            species_downstr varchar[],
            stock_upstr varchar[],
            stock_downstr varchar[],
            
            barriers_upstr varchar[],
            barriers_downstr varchar[],
            barrier_cnt_upstr integer,
            barrier_cnt_downstr integer,
            
            critical_habitat varchar[],
            
            passability_status varchar,
            last_inspection date,
            
            crossing_status varchar CHECK (crossing_status in ('MODELLED', 'ASSESSED', 'HABITAT_CONFIRMATION', 'DESIGN', 'REMEDIATED')),
            crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
            crossing_type varchar,
            crossing_subtype varchar,
            
            habitat_quality varchar,
            year_planned integer,
            year_complete integer,
            comments varchar,
            
            geometry geometry(Point, {appconfig.dataSrid}),
            
            primary key(id)
        );
        
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        


def computeCrossings(connection):
        
    query = f"""
        --roads
        INSERT INTO {dbTargetSchema}.{dbCrossingsTable} 
            (stream_name, strahler_order, stream_id, feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b."name" as feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{roadTable} b
                where st_intersects(a.geometry, b.geometry)
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, feature_name, 'ROAD', pnt 
            from points
        );
        
        
        
        --rail
        INSERT INTO {dbTargetSchema}.{dbCrossingsTable} 
            (stream_name, strahler_order, stream_id, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{railTable} b
                where st_intersects(a.geometry, b.geometry)
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, 'RAIL', pnt 
            from points
        );
        
        --trail
        INSERT INTO {dbTargetSchema}.{dbCrossingsTable} 
            (stream_name, strahler_order, stream_id, feature_name, crossing_feature_type, geometry) 
        
        (       
            with intersections as (       
                select st_intersection(a.geometry, b.geometry) as geometry,
                    a.id as stream_id, b.id as feature_id, 
                    a.stream_name, a.strahler_order, b."name" as feature_name
                from {dbTargetSchema}.{dbTargetStreamTable}  a,
                     {appconfig.dataSchema}.{trailTable} b
                where st_intersects(a.geometry, b.geometry)
            ),
            points as(
                select st_geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as pnt, * 
                from intersections
            )
            select stream_name, strahler_order, stream_id, feature_name, 'TRAIL', pnt 
            from points
        );
        
        
        UPDATE {dbTargetSchema}.{dbCrossingsTable} set stream_measure = 
        st_linelocatepoint(a.geometry, {dbTargetSchema}.{dbCrossingsTable}.geometry)
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id = {dbTargetSchema}.{dbCrossingsTable}.stream_id
        and {dbTargetSchema}.{dbCrossingsTable}.stream_id is not null;
        
        
        --populate species_upstr
        
        UPDATE {dbTargetSchema}.{dbCrossingsTable} set species_upstr =
        a.fish_survey_up
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id = {dbTargetSchema}.{dbCrossingsTable}.stream_id;
        
        --add species observation on the same edge as the stream
        --with a stream measure is greater than crossing measure 
        with edgespecies as (
            SELECT distinct  a.id, b.spec_code 
            FROM 
                {dbTargetSchema}.{dbCrossingsTable} a join 
                {dbTargetSchema}.{appconfig.config['DATABASE']['fish_survey_table']} b on a.stream_id = b.stream_id
            WHERE
                 a.stream_measure >= b.stream_measure and 
                 b.spec_code is not null
        ), 
        unqvalues as (
            SELECT id, array_agg(spec_code) as spec from edgespecies group by id
        )
        UPDATE {dbTargetSchema}.{dbCrossingsTable}
        SET species_upstr = species_upstr || spec
        FROM unqvalues where unqvalues.id = {dbTargetSchema}.{dbCrossingsTable}.id; 


        UPDATE {dbTargetSchema}.{dbCrossingsTable}
            SET species_upstr = ARRAY(SELECT DISTINCT UNNEST(species_upstr) order by 1); 
            
            
        --populate species_downstr
        
        UPDATE {dbTargetSchema}.{dbCrossingsTable} set species_downstr = a.fish_survey_down
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id = {dbTargetSchema}.{dbCrossingsTable}.stream_id;
        
        --add species observation on the same edge as the stream
        --with a stream measure is less than crossing measure 
        with edgespecies as (
            SELECT distinct  a.id, b.spec_code 
            FROM 
                {dbTargetSchema}.{dbCrossingsTable} a join 
                {dbTargetSchema}.{appconfig.config['DATABASE']['fish_survey_table']} b on a.stream_id = b.stream_id
            WHERE
                 a.stream_measure <= b.stream_measure and 
                 b.spec_code is not null
        ), 
        unqvalues as (
            SELECT id, array_agg(spec_code) as spec from edgespecies group by id
        )
        UPDATE {dbTargetSchema}.{dbCrossingsTable}
        SET species_downstr = species_downstr || spec
        FROM unqvalues where unqvalues.id = {dbTargetSchema}.{dbCrossingsTable}.id; 


        UPDATE {dbTargetSchema}.{dbCrossingsTable}
            SET species_downstr = ARRAY(SELECT DISTINCT UNNEST(species_downstr) order by 1); 
            
            
            
        --populate stock_upstr
        
        UPDATE {dbTargetSchema}.{dbCrossingsTable} set stock_upstr =
        a.fish_stock_up
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id = {dbTargetSchema}.{dbCrossingsTable}.stream_id;
        
        --add species observation on the same edge as the stream
        --with a stream measure is greater than crossing measure 
        with edgespecies as (
            SELECT distinct  a.id, b.spec_code 
            FROM 
                {dbTargetSchema}.{dbCrossingsTable} a join 
                {dbTargetSchema}.{appconfig.config['DATABASE']['fish_stocking_table']} b on a.stream_id = b.stream_id
            WHERE
                 a.stream_measure >= b.stream_measure and 
                 b.spec_code is not null
        ), 
        unqvalues as (
            SELECT id, array_agg(spec_code) as spec from edgespecies group by id
        )
        UPDATE {dbTargetSchema}.{dbCrossingsTable}
        SET stock_upstr = stock_upstr || spec
        FROM unqvalues where unqvalues.id = {dbTargetSchema}.{dbCrossingsTable}.id; 


        UPDATE {dbTargetSchema}.{dbCrossingsTable}
            SET stock_upstr = ARRAY(SELECT DISTINCT UNNEST(stock_upstr) order by 1); 
            
            
        --populate species_downstr
        
        UPDATE {dbTargetSchema}.{dbCrossingsTable} set stock_downstr = a.fish_stock_down
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id = {dbTargetSchema}.{dbCrossingsTable}.stream_id;
        
        --add species observation on the same edge as the stream
        --with a stream measure is less than crossing measure 
        with edgespecies as (
            SELECT distinct  a.id, b.spec_code 
            FROM 
                {dbTargetSchema}.{dbCrossingsTable} a join 
                {dbTargetSchema}.{appconfig.config['DATABASE']['fish_stocking_table']} b on a.stream_id = b.stream_id
            WHERE
                 a.stream_measure <= b.stream_measure and 
                 b.spec_code is not null
        ), 
        unqvalues as (
            SELECT id, array_agg(spec_code) as spec from edgespecies group by id
        )
        UPDATE {dbTargetSchema}.{dbCrossingsTable}
        SET stock_downstr = stock_downstr || spec
        FROM unqvalues where unqvalues.id = {dbTargetSchema}.{dbCrossingsTable}.id; 


        UPDATE {dbTargetSchema}.{dbCrossingsTable}
            SET stock_downstr = ARRAY(SELECT DISTINCT UNNEST(stock_downstr) order by 1); 
        
        
        --populate barrier fields - note the stream network is broken at barrier
        --points so we don't have to worry about stream measure issues like we do
        --with the stocking/survey points
        UPDATE {dbTargetSchema}.{dbCrossingsTable} SET 
            barriers_upstr = a.barriers_up,
            barriers_downstr = a.barriers_down,
            barrier_cnt_upstr = a.barrier_up_cnt,
            barrier_cnt_downstr = a.barrier_down_cnt
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id = {dbTargetSchema}.{dbCrossingsTable}.stream_id;
    """
    #print(query)
    with connection.cursor() as cursor:
        cursor.execute(query)
                        
#--- main program ---    
with appconfig.connectdb() as conn:
    
    conn.autocommit = False
    
    print("Computing Modelled Crossings")
    
    print("  creating tables")
    createTable(conn)
    
    print("  computing modelled crossings")
    computeCrossings(conn)
    
print("done")

