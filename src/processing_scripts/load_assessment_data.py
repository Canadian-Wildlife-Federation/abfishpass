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
# This script loads an assessment data file into the database, joins
# the points to modelled crossings based on a specified buffer distance,
# loads the joined and modelled points to the crossings table,
# and finally loads crossing points to the barriers table.
#
# The script assumes assessment data files only contain data for a single
# HUC 8 watershed.
#

import subprocess
import appconfig

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbWatershedId = appconfig.config[iniSection]['watershed_id']
rawData = appconfig.config[iniSection]['assessment_data']
dataSchema = appconfig.config['DATABASE']['data_schema']

dbTempTable = 'assessment_data_' + dbWatershedId
dbTargetTable = appconfig.config['CROSSINGS']['assessed_crossings_table']
dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']
dbCrossingsTable = appconfig.config['CROSSINGS']['crossings_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
watershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_table']
joinDistance = appconfig.config['CROSSINGS']['join_distance']

def loadAssessmentData(connection):
        
    # create assessed crossings table
    query = f"""
        DROP TABLE IF EXISTS {dataSchema}.{dbTempTable};
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetTable};
        
        CREATE TABLE {dbTargetSchema}.{dbTargetTable} (
            unique_id uuid default gen_random_uuid(),
            culvert_number varchar,
            structure_id varchar,
            date_examined date,
            examiners varchar,
            latitude double precision,
            longitude double precision,
            road varchar,
            structure_type varchar,
            culvert_condition varchar,
            comments varchar,
            passability_status varchar,
            stream_name varchar,
            "owner" varchar,
            crossing_type varchar,
            crossing_subtype varchar,
            crossing_status varchar,
            geometry geometry(POINT, {appconfig.dataSrid}),

            primary key (unique_id)
        )
        
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

    # load assessment data
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + dataSchema + '.' + dbTempTable + '" -lco GEOMETRY_NAME=geometry "' + rawData + '" -oo EMPTY_STRING_AS_NULL=YES'
    # print(pycmd)
    subprocess.run(pycmd)

    query = f"""
        INSERT INTO {dbTargetSchema}.{dbTargetTable} (
            culvert_number,
            structure_id,
            date_examined,
            examiners,
            latitude,
            longitude,
            road,
            structure_type,
            culvert_condition,
            comments,
            passability_status,
            geometry
            )
        SELECT 
            culvert_number,
            structure_id,
            date_examined::date,
            examiners,
            latitude,
            longitude,
            road,
            structure_type,
            culvert_condition,
            comments,
            passability_status,
            geometry
        FROM {dataSchema}.{dbTempTable};

        -- TO DO: update this after we see what information we have in assessment data
        -- UPDATE {dbTargetSchema}.{dbTargetTable}
        -- SET crossing_subtype = 
        --     CASE
        --     WHEN crossing_subtype ILIKE 'bridge%' THEN 'bridge'
        --     WHEN crossing_subtype ILIKE 'culvert%' THEN 'culvert'
        --     WHEN crossing_subtype ILIKE 'ford%' THEN 'ford'
        --     WHEN crossing_subtype IS NULL OR crossing_subtype = 'No crossing present' THEN NULL
        --     ELSE 'other' END;

    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def joinAssessmentData(connection):

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbCrossingsTable};

        CREATE TABLE {dbTargetSchema}.{dbCrossingsTable} (
        modelled_id uuid,
        assessment_id uuid,
        stream_name varchar,
        strahler_order integer,
        stream_id uuid,
        wshed_name varchar,
        transport_feature_name varchar,
        passability_status varchar,

        crossing_status varchar CHECK (crossing_status in ('MODELLED', 'ASSESSED', 'HABITAT_CONFIRMATION', 'DESIGN', 'REMEDIATED')),
        crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
        crossing_type varchar,
        crossing_subtype varchar,

        owner varchar,
        culvert_number varchar,
        structure_id varchar,
        date_examined date,
        examiners varchar,
        structure_type varchar,
        culvert_condition varchar,
        comments varchar,

        geometry geometry(Point, {appconfig.dataSrid}),
        
        primary key (modelled_id)
        );

        -- add modelled crossings
        INSERT INTO {dbTargetSchema}.{dbCrossingsTable} (
            modelled_id,
            stream_name,
            strahler_order,
            stream_id,
            transport_feature_name,
            passability_status,

            crossing_status,
            crossing_feature_type,
            crossing_type,
            crossing_subtype,
            geometry
        )
        SELECT
            modelled_id,
            stream_name,
            strahler_order,
            stream_id,
            transport_feature_name,
            passability_status,

            crossing_status,
            crossing_feature_type,
            crossing_type,
            crossing_subtype,
            geometry
        FROM {dbTargetSchema}.{dbModelledCrossingsTable};

        --match assessment data to modelled points
        --TO DO: figure out if any assessment data does not / should not match a modelled point
        with match AS (
            SELECT
                DISTINCT ON (assess.unique_id) assess.unique_id AS unique_id, model.modelled_id AS modelled_id, ST_Distance(model.geometry, assess.geometry) AS dist
            FROM {dbTargetSchema}.{dbTargetTable} AS assess, {dbTargetSchema}.{dbModelledCrossingsTable} AS model
            WHERE ST_DWithin(model.geometry, assess.geometry, {joinDistance})
            ORDER BY unique_id, modelled_id, ST_Distance(model.geometry, assess.geometry)
            )
        UPDATE {dbTargetSchema}.{dbCrossingsTable}
        SET assessment_id = a.unique_id
        FROM match AS a WHERE a.modelled_id = {dbTargetSchema}.{dbCrossingsTable}.modelled_id;

        UPDATE {dbTargetSchema}.{dbCrossingsTable} AS b
        SET
            culvert_number = CASE WHEN a.culvert_number IS NOT NULL THEN a.culvert_number ELSE b.culvert_number END,
            structure_id = CASE WHEN a.structure_id IS NOT NULL THEN a.structure_id ELSE b.structure_id END,
            date_examined = CASE WHEN a.date_examined IS NOT NULL THEN a.date_examined ELSE b.date_examined END,
            examiners = CASE WHEN a.examiners IS NOT NULL THEN a.examiners ELSE b.examiners END,
            transport_feature_name = CASE WHEN (a.road IS NOT NULL AND a.road IS DISTINCT FROM b.transport_feature_name) THEN a.road ELSE b.transport_feature_name END,
            structure_type = CASE WHEN a.structure_type IS NOT NULL THEN a.structure_type ELSE b.structure_type END,
            culvert_condition = CASE WHEN a.culvert_condition IS NOT NULL THEN a.culvert_condition ELSE b.culvert_condition END,
            "comments" = CASE WHEN a.comments IS NOT NULL THEN a.comments ELSE b.comments END,
            passability_status = CASE WHEN a.passability_status IS NOT NULL THEN a.passability_status ELSE b.passability_status END,
            crossing_status = 'ASSESSED'
        FROM {dbTargetSchema}.{dbTargetTable} AS a
        WHERE b.assessment_id = a.unique_id;

    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def loadToBarriers(connection):

    query = f"""
        DELETE FROM {dbTargetSchema}.{dbBarrierTable} WHERE type = 'stream_crossing';
        
        INSERT INTO {dbTargetSchema}.{dbBarrierTable}(
            modelled_id, assessment_id, snapped_point,
            type, owner, passability_status,
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype, culvert_number,
            structure_id, date_examined, examiners,
            structure_type, culvert_condition,
            "comments"
        )
        SELECT 
            modelled_id, assessment_id, geometry,
            'stream_crossing', owner, passability_status,
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype, culvert_number,
            structure_id, date_examined, examiners,
            structure_type, culvert_condition,
            "comments"
        FROM {dbTargetSchema}.{dbCrossingsTable};

        -- TO DO: change this from hardcoded value if we need to separate watersheds
        UPDATE {dbTargetSchema}.{dbBarrierTable} SET wshed_name = '01cd000';

    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

#--- main program ---
def main():
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Loading Assessment Data for Stream Crossings")
        
        print("  loading assessment data")
        loadAssessmentData(conn)
        
        print("  joining assessment points to modelled points")
        joinAssessmentData(conn)
        
        print("  adding joined points to crossings and barriers tables")
        loadToBarriers(conn)  
        
    print("done")
    
if __name__ == "__main__":
    main()   