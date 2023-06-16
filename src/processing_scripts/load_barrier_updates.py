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
# This script loads a barrier updates file into the database, and
# joins these updates to their respective tables. It can add, delete,
# and modify features of any barrier type.
#
# The script assumes the barrier updates file only contains data
# for a single watershed.
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
        
    # load updates into a table
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + dbTargetSchema + '.' + dbTargetTable + '" -lco GEOMETRY_NAME=geometry "' + rawData + '" -oo EMPTY_STRING_AS_NULL=YES'
    subprocess.run(pycmd)

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD COLUMN update_id uuid default gen_random_uuid();
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} DROP CONSTRAINT IF EXISTS {dbTargetTable}_pkey;
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD CONSTRAINT {dbTargetTable}_pkey PRIMARY KEY (update_id);
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def joinBarrierUpdates(connection):

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD COLUMN barrier_id uuid;
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)

    query = f"""
        SELECT DISTINCT barrier_type
        FROM {dbTargetSchema}.{dbTargetTable};
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        barrierTypes = cursor.fetchall()

    for bType in barrierTypes:
        barrier = bType[0]
        query = f"""
        with match AS (
            SELECT
            foo.update_id,
            closest_point.id,
            closest_point.dist
            FROM {dbTargetSchema}.{dbTargetTable} AS foo
            CROSS JOIN LATERAL 
            (SELECT
                id, 
                ST_Distance(bar.snapped_point, foo.geometry) as dist
                FROM {dbTargetSchema}.{dbBarrierTable} AS bar
                WHERE ST_DWithin(bar.snapped_point, foo.geometry, {joinDistance})
                AND bar.type = '{barrier}'
                ORDER BY ST_Distance(bar.snapped_point, foo.geometry)
                LIMIT 1
            ) AS closest_point
            WHERE foo.barrier_type = '{barrier}'
            )
        UPDATE {dbTargetSchema}.{dbTargetTable}
        SET barrier_id = a.id
        FROM match AS a WHERE a.update_id = {dbTargetSchema}.{dbTargetTable}.update_id;
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
    
    connection.commit()

def processUpdates(connection):

    def processMultiple(connection):

        # where multiple updates exist for a feature, only update one at a time
        waitCount = 0
        waitQuery = f"""SELECT COUNT(*) FROM {dbTargetSchema}.{dbTargetTable} WHERE update_status = 'wait'"""

        while(True):
            with connection.cursor() as cursor:
                cursor.execute(initializeQuery)
                cursor.execute(waitQuery)
                waitCount = int(cursor.fetchone()[0])
                print("   ", waitCount, "updates are waiting to be made...")

                # update most fields
                cursor.execute(mappingQuery)
                # update species-specific passability fields
                for species in specCodes:
                    code = species[0]
                    colname = "passability_status_" + code
                    passabilityQuery = f"""
                        UPDATE {dbTargetSchema}.{dbBarrierTable} AS b
                        SET {colname} = CASE WHEN a.{colname} IS NOT NULL THEN UPPER(a.{colname}) ELSE b.{colname} END
                        FROM {dbTargetSchema}.{dbTargetTable} AS a
                        WHERE b.id = a.barrier_id
                        AND a.update_status = 'ready';
                    """
                    cursor.execute(passabilityQuery)

                query = f"""
                    UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'done' WHERE update_status = 'ready';
                    UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'ready' WHERE update_status = 'wait';
                """
                cursor.execute(query)
            
                connection.commit()

            if waitCount == 0:
                break

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} ADD COLUMN update_status varchar;
        UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'ready';
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    
    initializeQuery = f"""
        WITH cte AS (
        SELECT update_id, barrier_id,
            row_number() OVER(PARTITION BY barrier_id ORDER BY update_date ASC) AS rn
        FROM {dbTargetSchema}.{dbTargetTable} WHERE update_status = 'ready'
        AND update_type = 'modify feature'
        )
        UPDATE {dbTargetSchema}.{dbTargetTable}
        SET update_status = 'wait'
            WHERE update_id IN (SELECT update_id FROM cte WHERE rn > 1);
    """
    with connection.cursor() as cursor:
        cursor.execute(initializeQuery)

    newCols = []
    for species in specCodes:
        code = species[0]
        col = "passability_status_" + code
        newCols.append(col)
    colString = ','.join(newCols)
    prefix = "UPPER("
    suffix = ")"
    colStringUpper = ','.join([f'{prefix}{col}{suffix}' for col in newCols])

    mappingQuery = f"""
        -- new points
        INSERT INTO {dbTargetSchema}.{dbBarrierTable} (
            update_id, original_point, type,
            {colString}, passability_status_notes,
            culvert_number, structure_id, date_examined,
            transport_feature_name, culvert_type,
            culvert_condition, action_items
            )
        SELECT 
            update_id, geometry, barrier_type,
            {colStringUpper}, passability_status_notes,
            culvert_number, structure_id, date_examined,
            road, culvert_type,
            culvert_condition, action_items
        FROM {dbTargetSchema}.{dbTargetTable}
        WHERE update_type = 'new feature'
        AND update_status = 'ready';

        UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'done' WHERE update_type = 'new feature';

        -- deleted points
        DELETE FROM {dbTargetSchema}.{dbBarrierTable}
        WHERE id IN (
            SELECT barrier_id FROM {dbTargetSchema}.{dbTargetTable}
            WHERE update_type = 'delete feature'
            AND update_status = 'ready'
            );
        
        UPDATE {dbTargetSchema}.{dbTargetTable} SET update_status = 'done' WHERE update_type = 'delete feature';

        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');
        UPDATE {dbTargetSchema}.{dbBarrierTable} SET snapped_point = original_point WHERE snapped_point IS NULL;

        -- updated points
        UPDATE {dbTargetSchema}.{dbBarrierTable} AS b SET update_id = 
            CASE
            WHEN b.update_id IS NULL THEN a.update_id::varchar
            WHEN b.update_id IS NOT NULL THEN b.update_id::varchar || ',' || a.update_id::varchar
            ELSE NULL END
            FROM {dbTargetSchema}.{dbTargetTable} AS a
            WHERE b.id = a.barrier_id
            AND a.update_status = 'ready';

        UPDATE {dbTargetSchema}.{dbBarrierTable} AS b
        SET
            culvert_number = CASE WHEN a.culvert_number IS NOT NULL THEN a.culvert_number ELSE b.culvert_number END,
            structure_id = CASE WHEN a.structure_id IS NOT NULL THEN a.structure_id ELSE b.structure_id END,
            date_examined = CASE WHEN a.date_examined IS NOT NULL THEN a.date_examined ELSE b.date_examined END,
            transport_feature_name = CASE WHEN (a.road IS NOT NULL AND a.road IS DISTINCT FROM b.transport_feature_name) THEN a.road ELSE b.transport_feature_name END,
            culvert_type = CASE WHEN a.culvert_type IS NOT NULL THEN a.culvert_type ELSE b.culvert_type END,
            culvert_condition = CASE WHEN a.culvert_condition IS NOT NULL THEN a.culvert_condition ELSE b.culvert_condition END,
            passability_status_notes =
                CASE
                WHEN a.passability_status_notes IS NOT NULL AND b.passability_status_notes IS NULL THEN a.passability_status_notes
                WHEN a.passability_status_notes IS NOT NULL AND b.passability_status_notes IS NOT NULL THEN b.passability_status_notes || ';' || a.passability_status_notes
                ELSE b.passability_status_notes END,
            action_items = CASE WHEN a.action_items IS NOT NULL THEN a.action_items ELSE b.action_items END,
            crossing_subtype = CASE WHEN a.crossing_subtype IS NOT NULL THEN a.crossing_subtype ELSE b.crossing_subtype END
        FROM {dbTargetSchema}.{dbTargetTable} AS a
        WHERE b.id = a.barrier_id
        AND a.update_status = 'ready';
    """

    processMultiple(connection)

#--- main program ---
def main():
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False

        print("Loading Barrier Updates")
        loadBarrierUpdates(conn)
        
        print("  joining update points to barriers")
        joinBarrierUpdates(conn)
        
        print("  processing updates")
        processUpdates(conn)
        
    print("done")
    
if __name__ == "__main__":
    main()   