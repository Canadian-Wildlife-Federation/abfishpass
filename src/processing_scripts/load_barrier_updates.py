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
rawData = appconfig.config[iniSection]['barrier_updates']
dataSchema = appconfig.config['DATABASE']['data_schema']

dbTempTable = 'barrier_updates_' + dbWatershedId
dbTargetTable = appconfig.config['BARRIER_PROCESSING']['barrier_updates_table']
dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']
dbCrossingsTable = appconfig.config['CROSSINGS']['crossings_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
watershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_table']
joinDistance = appconfig.config['CROSSINGS']['join_distance']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

with appconfig.connectdb() as conn:

    query = f"""
    SELECT code
    FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        specCodes = cursor.fetchall()

def loadBarrierUpdates(connection):
        
    # create assessed crossings table
    # TO DO: add handling for beaver activity and dam updates
    # TO DO: add handling for new and deleted points
    query = f"""
        DROP TABLE IF EXISTS {dataSchema}.{dbTempTable};
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetTable};
        
        CREATE TABLE {dbTargetSchema}.{dbTargetTable} (
            update_id uuid default gen_random_uuid(),
            culvert_number varchar,
            structure_id varchar,
            date_examined date,
            latitude double precision,
            longitude double precision,
            road varchar,
            culvert_type varchar,
            culvert_condition varchar,
            passability_status_notes varchar,
            action_items varchar,
            stream_name varchar,
            "owner" varchar,
            crossing_type varchar,
            crossing_subtype varchar,
            crossing_status varchar,
            geometry geometry(POINT, {appconfig.dataSrid}),

            primary key (update_id)
        )
        
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()
        
    for species in specCodes:
        code = species[0]

        colname = "passability_status_" + code
        
        query = f"""
            alter table {dbTargetSchema}.{dbTargetTable} 
            add column if not exists {colname} varchar;
        """

        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()

    # load assessment data
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + dataSchema + '.' + dbTempTable + '" -lco GEOMETRY_NAME=geometry "' + rawData + '" -oo EMPTY_STRING_AS_NULL=YES'
    # print(pycmd)
    subprocess.run(pycmd)

    colNames = []
    
    for species in specCodes:
        code = species[0]

        col = "passability_status_" + code
        colNames.append(col)
    
    colString = ','.join(colNames)
        
    query = f"""
        INSERT INTO {dbTargetSchema}.{dbTargetTable} (
            culvert_number,
            structure_id,
            date_examined,
            latitude,
            longitude,
            road,
            culvert_type,
            culvert_condition,
            {colString},
            passability_status_notes,
            action_items,
            geometry
            )
        SELECT 
            culvert_number,
            structure_id,
            date_examined::date,
            latitude,
            longitude,
            road,
            culvert_type,
            culvert_condition,
            {colString},
            passability_status_notes,
            action_items,
            geometry
        FROM {dataSchema}.{dbTempTable};

        UPDATE {dbTargetSchema}.{dbTargetTable} SET crossing_subtype = 'culvert' where culvert_number ILIKE '%culvert%';

    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def joinAssessmentData(connection):

    newCols = []

    for species in specCodes:
        code = species[0]

        col = "passability_status_" + code + " varchar"
        newCols.append(col)
    
    colString = ','.join(newCols) # string including column type
    colStringSimple = colString.replace(" varchar", "") # string without column type

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbCrossingsTable};

        CREATE TABLE {dbTargetSchema}.{dbCrossingsTable} (
        id serial not null,
        modelled_id uuid,
        update_id uuid,
        stream_name varchar,
        strahler_order integer,
        stream_id uuid,
        wshed_name varchar,
        transport_feature_name varchar,
        {colString},
        passability_status_notes varchar,

        crossing_status varchar CHECK (crossing_status in ('MODELLED', 'ASSESSED', 'HABITAT_CONFIRMATION', 'DESIGN', 'REMEDIATED')),
        crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
        crossing_type varchar,
        crossing_subtype varchar,

        owner varchar,
        culvert_number varchar,
        structure_id varchar,
        date_examined date,
        culvert_type varchar,
        culvert_condition varchar,
        action_items varchar,

        geometry geometry(Point, {appconfig.dataSrid}),
        
        primary key (id)
        );

        -- add modelled crossings
        INSERT INTO {dbTargetSchema}.{dbCrossingsTable} (
            modelled_id,
            stream_name,
            strahler_order,
            stream_id,
            transport_feature_name,
            {colStringSimple},
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
            {colStringSimple},
            crossing_status,
            crossing_feature_type,
            crossing_type,
            crossing_subtype,
            geometry
        FROM {dbTargetSchema}.{dbModelledCrossingsTable};

        --match assessment data to modelled points
        with match AS (
            SELECT
                DISTINCT ON (assess.update_id) assess.update_id AS update_id, model.modelled_id AS modelled_id, ST_Distance(model.geometry, assess.geometry) AS dist
            FROM {dbTargetSchema}.{dbTargetTable} AS assess, {dbTargetSchema}.{dbModelledCrossingsTable} AS model
            WHERE ST_DWithin(model.geometry, assess.geometry, {joinDistance})
            ORDER BY update_id, modelled_id, ST_Distance(model.geometry, assess.geometry)
            )
        UPDATE {dbTargetSchema}.{dbCrossingsTable}
        SET update_id = a.update_id
        FROM match AS a WHERE a.modelled_id = {dbTargetSchema}.{dbCrossingsTable}.modelled_id;

        --add any assessment points that could not be matched - just 1 in current assessment data
        INSERT INTO {dbTargetSchema}.{dbCrossingsTable} (
            update_id,
            transport_feature_name,
            {colStringSimple},
            passability_status_notes,
            crossing_subtype,
            culvert_number,
            structure_id,
            date_examined,
            culvert_type,
            culvert_condition,
            action_items,
            geometry
        )
        SELECT
            update_id,
            road,
            {colStringSimple},
            passability_status_notes,
            crossing_subtype,
            culvert_number,
            structure_id,
            date_examined,
            culvert_type,
            culvert_condition,
            action_items,
            geometry
        FROM {dbTargetSchema}.{dbTargetTable}
        WHERE update_id NOT IN (SELECT update_id FROM {dbTargetSchema}.{dbCrossingsTable} WHERE update_id IS NOT NULL);

        
        UPDATE {dbTargetSchema}.{dbCrossingsTable} AS b
        SET
            culvert_number = CASE WHEN a.culvert_number IS NOT NULL THEN a.culvert_number ELSE b.culvert_number END,
            structure_id = CASE WHEN a.structure_id IS NOT NULL THEN a.structure_id ELSE b.structure_id END,
            date_examined = CASE WHEN a.date_examined IS NOT NULL THEN a.date_examined ELSE b.date_examined END,
            transport_feature_name = CASE WHEN (a.road IS NOT NULL AND a.road IS DISTINCT FROM b.transport_feature_name) THEN a.road ELSE b.transport_feature_name END,
            culvert_type = CASE WHEN a.culvert_type IS NOT NULL THEN a.culvert_type ELSE b.culvert_type END,
            culvert_condition = CASE WHEN a.culvert_condition IS NOT NULL THEN a.culvert_condition ELSE b.culvert_condition END,
            passability_status_notes = CASE WHEN a.passability_status_notes IS NOT NULL THEN a.passability_status_notes ELSE b.passability_status_notes END,
            action_items = CASE WHEN a.action_items IS NOT NULL THEN a.action_items ELSE b.action_items END,
            crossing_status = CASE WHEN a.culvert_number IS NOT NULL THEN 'ASSESSED' ELSE b.crossing_status END
        FROM {dbTargetSchema}.{dbTargetTable} AS a
        WHERE b.update_id = a.update_id;

    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

    # update species-specific passability fields
    for species in specCodes:
        code = species[0]
        colname = "passability_status_" + code

        query = f"""
            UPDATE {dbTargetSchema}.{dbCrossingsTable} AS b
            SET {colname} = CASE WHEN a.{colname} IS NOT NULL THEN UPPER(a.{colname}) ELSE b.{colname} END
            FROM {dbTargetSchema}.{dbTargetTable} AS a
            WHERE b.update_id = a.update_id;
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
            modelled_id, update_id, snapped_point,
            type, owner, {colString}, passability_status_notes,
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype, culvert_number,
            structure_id, date_examined,
            culvert_type, culvert_condition, action_items
        )
        SELECT 
            modelled_id, update_id, geometry,
            'stream_crossing', owner, {colString}, passability_status_notes,
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype, culvert_number,
            structure_id, date_examined,
            culvert_type, culvert_condition, action_items
        FROM {dbTargetSchema}.{dbCrossingsTable};

        UPDATE {dbTargetSchema}.{dbBarrierTable} SET wshed_name = '{dbWatershedId}';
        
        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

#--- main program ---
def main():
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False

        print("Loading Barrier Updates")
        loadBarrierUpdates(conn)
        
        print("  joining update points to barriers")
        joinAssessmentData(conn)
        
        print("  adding joined points to barriers tables")
        loadToBarriers(conn)
        
    print("done")
    
if __name__ == "__main__":
    main()   
