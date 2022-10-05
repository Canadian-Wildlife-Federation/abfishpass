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
joinDistance = appconfig.config['CROSSINGS']['join_distance']

def loadAssessmentData(connection):
        
    # create assessed crossings table
    query = f"""
        DROP TABLE IF EXISTS {dataSchema}.{dbTempTable};
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetTable};
        
        CREATE TABLE {dbTargetSchema}.{dbTargetTable} (
            unique_id uuid default uuid_generate_v4(),
            disp_num varchar,
            stream_name varchar,
            ownership_type varchar,
            crossing_type varchar,
            crossing_subtype varchar,
            crossing_status varchar,
            last_inspection date,
            passability_status varchar,
            habitat_quality varchar,
            year_planned numeric,
            year_complete numeric,
            comments varchar,
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

    # TO DO: create new copy of predicted_points table where owner is 
    # converted to ownership_type
    query = f"""
        INSERT INTO {dbTargetSchema}.{dbTargetTable} (
            disp_num,
            stream_name,
            ownership_type,
            crossing_type,
            crossing_subtype,
            crossing_status,
            last_inspection,
            passability_status,
            habitat_quality,
            year_planned,
            year_complete,
            comments,
            geometry
            )
        SELECT 
            DISP_NUM,
            StreamName,
            OWNER,
            CASE WHEN CrossingType ILIKE 'bridge%' THEN 'obs' ELSE NULL END,
            CrossingType,
            CASE WHEN inspected = 'YES' OR lastinspection IS NOT NULL THEN 'ASSESSED' ELSE NULL END,
            lastinspection,
            CASE WHEN fishpassage = 'No Concerns' THEN 'PASSABLE' ELSE 'BARRIER' END,
            habitatquality,
            CASE WHEN year_planned IS NOT NULL AND year_planned != -1 THEN year_planned ELSE NULL END,
            CASE WHEN year_complete IS NOT NULL AND year_complete != -1 THEN year_complete ELSE NULL END,
            comments,
            geometry
        FROM {dataSchema}.{dbTempTable};

        UPDATE {dbTargetSchema}.{dbTargetTable}
        SET crossing_subtype = 
            CASE
            WHEN crossing_subtype ILIKE 'bridge%' THEN 'bridge'
            WHEN crossing_subtype ILIKE 'culvert%' THEN 'culvert'
            WHEN crossing_subtype ILIKE 'ford%' THEN 'ford'
            WHEN crossing_subtype IS NULL OR crossing_subtype = 'No crossing present' THEN NULL
            ELSE 'other' END

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
        disp_num varchar,
        stream_name varchar,
        strahler_order integer,
        stream_id uuid, 
        stream_measure numeric,
        wshed_name varchar,
        wshed_priority varchar,
        transport_feature_name varchar,
        ownership_type varchar,
        
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
        
        primary key (modelled_id)
        );

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
            disp_num = CASE WHEN a.disp_num IS NOT NULL THEN a.disp_num ELSE b.disp_num END,
            ownership_type = CASE WHEN a.ownership_type IS NOT NULL THEN a.ownership_type ELSE b.ownership_type END,
            crossing_type = CASE WHEN a.crossing_type IS NOT NULL THEN a.crossing_type ELSE b.crossing_type END,
            crossing_subtype = CASE WHEN a.crossing_subtype IS NOT NULL THEN a.crossing_subtype ELSE b.crossing_subtype END,
            crossing_status = CASE WHEN a.crossing_status IS NOT NULL THEN a.crossing_status ELSE b.crossing_status END,
            last_inspection = CASE WHEN a.last_inspection IS NOT NULL THEN a.last_inspection ELSE b.last_inspection END,
            passability_status = CASE WHEN a.passability_status IS NOT NULL THEN a.passability_status ELSE b.passability_status END,
            habitat_quality = CASE WHEN a.habitat_quality IS NOT NULL THEN a.habitat_quality ELSE b.habitat_quality END,
            year_planned = CASE WHEN a.year_planned IS NOT NULL THEN a.year_planned ELSE b.year_planned END,
            year_complete = CASE WHEN a.year_complete IS NOT NULL THEN a.year_complete ELSE b.year_complete END,
            comments = CASE WHEN a.comments IS NOT NULL THEN a.comments ELSE b.comments END
        FROM {dbTargetSchema}.{dbTargetTable} AS a
        WHERE b.assessment_id = a.unique_id;

    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

# def loadToBarriers(connection):

#--- main program ---
def main():
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Loading Assessment Data for Stream Crossings")
        
        print("  loading assessment data")
        loadAssessmentData(conn)
        
        print("  joining assessment points to modelled points")
        joinAssessmentData(conn)
        
        # print("  adding joined points to crossings and barriers tables")
        # loadToBarriers(conn)  
        
    print("done")
    
if __name__ == "__main__":
    main()   