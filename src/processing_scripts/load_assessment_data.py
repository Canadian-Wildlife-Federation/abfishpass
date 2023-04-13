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
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

def loadAssessmentData(connection):
        
    # create assessed crossings table
    query = f"""
        DROP TABLE IF EXISTS {dataSchema}.{dbTempTable};
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetTable};
        
        CREATE TABLE {dbTargetSchema}.{dbTargetTable} (
            assessment_id uuid default gen_random_uuid(),
            culvert_number varchar,
            structure_id varchar,
            date_examined date,
            examiners varchar,
            latitude double precision,
            longitude double precision,
            road varchar,
            structure_type varchar,
            culvert_condition varchar,
            passability_status varchar,
            passability_status_notes varchar,
            action_items varchar,
            stream_name varchar,
            "owner" varchar,
            crossing_type varchar,
            crossing_subtype varchar,
            crossing_status varchar,
            geometry geometry(POINT, {appconfig.dataSrid}),

            primary key (assessment_id)
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
            passability_status,
            passability_status_notes,
            action_items,
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
            passability_status,
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

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbCrossingsTable};

        CREATE TABLE {dbTargetSchema}.{dbCrossingsTable} (
        id serial not null,
        modelled_id uuid,
        assessment_id uuid,
        stream_name varchar,
        strahler_order integer,
        stream_id uuid,
        wshed_name varchar,
        transport_feature_name varchar,
        passability_status varchar,
        passability_status_notes varchar,

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
        with match AS (
            SELECT
                DISTINCT ON (assess.assessment_id) assess.assessment_id AS assessment_id, model.modelled_id AS modelled_id, ST_Distance(model.geometry, assess.geometry) AS dist
            FROM {dbTargetSchema}.{dbTargetTable} AS assess, {dbTargetSchema}.{dbModelledCrossingsTable} AS model
            WHERE ST_DWithin(model.geometry, assess.geometry, {joinDistance})
            ORDER BY assessment_id, modelled_id, ST_Distance(model.geometry, assess.geometry)
            )
        UPDATE {dbTargetSchema}.{dbCrossingsTable}
        SET assessment_id = a.assessment_id
        FROM match AS a WHERE a.modelled_id = {dbTargetSchema}.{dbCrossingsTable}.modelled_id;

        --add any assessment points that could not be matched - just 1 in current assessment data
        INSERT INTO {dbTargetSchema}.{dbCrossingsTable} (
            assessment_id,
            transport_feature_name,
            passability_status,
            passability_status_notes,
            crossing_subtype,
            culvert_number,
            structure_id,
            date_examined,
            examiners,
            structure_type,
            culvert_condition,
            action_items,
            geometry
        )
        SELECT
            assessment_id,
            road,
            passability_status,
            passability_status_notes,
            crossing_subtype,
            culvert_number,
            structure_id,
            date_examined,
            examiners,
            structure_type,
            culvert_condition,
            action_items,
            geometry
        FROM {dbTargetSchema}.{dbTargetTable}
        WHERE assessment_id NOT IN (SELECT assessment_id FROM {dbTargetSchema}.{dbCrossingsTable} WHERE assessment_id IS NOT NULL);

        
        UPDATE {dbTargetSchema}.{dbCrossingsTable} AS b
        SET
            culvert_number = CASE WHEN a.culvert_number IS NOT NULL THEN a.culvert_number ELSE b.culvert_number END,
            structure_id = CASE WHEN a.structure_id IS NOT NULL THEN a.structure_id ELSE b.structure_id END,
            date_examined = CASE WHEN a.date_examined IS NOT NULL THEN a.date_examined ELSE b.date_examined END,
            examiners = CASE WHEN a.examiners IS NOT NULL THEN a.examiners ELSE b.examiners END,
            transport_feature_name = CASE WHEN (a.road IS NOT NULL AND a.road IS DISTINCT FROM b.transport_feature_name) THEN a.road ELSE b.transport_feature_name END,
            structure_type = CASE WHEN a.structure_type IS NOT NULL THEN a.structure_type ELSE b.structure_type END,
            culvert_condition = CASE WHEN a.culvert_condition IS NOT NULL THEN a.culvert_condition ELSE b.culvert_condition END,
            passability_status = CASE WHEN a.passability_status IS NOT NULL THEN UPPER(a.passability_status) ELSE b.passability_status END,
            passability_status_notes = CASE WHEN a.passability_status_notes IS NOT NULL THEN a.passability_status_notes ELSE b.passability_status_notes END,
            action_items = CASE WHEN a.action_items IS NOT NULL THEN a.action_items ELSE b.action_items END,
            crossing_status = CASE WHEN a.culvert_number IS NOT NULL THEN 'ASSESSED' ELSE b.crossing_status END
        FROM {dbTargetSchema}.{dbTargetTable} AS a
        WHERE b.assessment_id = a.assessment_id;

    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def loadToBarriers(connection):

    query = f"""
        DELETE FROM {dbTargetSchema}.{dbBarrierTable} WHERE type = 'stream_crossing';
        
        INSERT INTO {dbTargetSchema}.{dbBarrierTable}(
            modelled_id, assessment_id, snapped_point,
            type, owner, passability_status, passability_status_notes,
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype, culvert_number,
            structure_id, date_examined, examiners,
            structure_type, culvert_condition, action_items
        )
        SELECT 
            modelled_id, assessment_id, geometry,
            'stream_crossing', owner, passability_status, passability_status_notes,
            stream_name, strahler_order, stream_id, 
            transport_feature_name, crossing_status,
            crossing_feature_type, crossing_type,
            crossing_subtype, culvert_number,
            structure_id, date_examined, examiners,
            structure_type, culvert_condition, action_items
        FROM {dbTargetSchema}.{dbCrossingsTable};

        UPDATE {dbTargetSchema}.{dbBarrierTable} SET wshed_name = '{dbWatershedId}';
        
        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');

        DELETE FROM {dbTargetSchema}.{dbBarrierTable}
            WHERE modelled_id IN (
            'd067a497-3ed4-40ae-8bf3-03b2a3701468',
            '684384af-e469-44ed-afb5-209a41b8a38a'
        );

        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET passability_status = 'PASSABLE',
                passability_status_notes = 'Likely partial barrier due to fishway presence'
            WHERE cabd_id = '04e4fc7c-e418-4fc3-b083-806f0b1f5c3c';

        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET passability_status = 'PASSABLE',
                passability_status_notes = 'Marked as passable by CWF due to upstream spawning observations for Atlantic salmon'
            WHERE cabd_id IN ('179709a4-6aa7-4545-9271-446adc3f6cd9', 'a3da4307-64dc-4fbf-9514-8298429a6bc8');

        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET passability_status = 'PASSABLE',
                passability_status_notes = 'Marked as passable by CWF due to upstream spawning observations for Atlantic salmon'
            WHERE modelled_id IN (
            '9247ac5e-a030-4d35-9de6-cfb096e4ed3a',
            'b1bb1547-a108-429d-b89a-13db3cacac0b',
            'f4c67968-2839-4b05-aa89-2e30ca7506bb',
            'd067a497-3ed4-40ae-8bf3-03b2a3701468',
            '5be7c620-f059-4cde-8f9e-d467d51ad956',
            '99c1df3d-1041-4e49-bbf0-1e26f735479e',
            'aa505c04-e809-44c5-a3ee-fa97b0fe6ff9',
            '684384af-e469-44ed-afb5-209a41b8a38a',
            '5589d332-da33-4664-b78f-cdad70205359',
            '1d3cb879-0764-47a5-b7be-a561f8db150f',
            '5b4a85cf-afaf-4de1-8768-f1c50034210c',
            '1d56009a-f5ec-458e-b512-3b395bb64516',
            '6b85e3db-27bc-4e69-a2d0-6591283f8e6c',
            '0ef6f6d3-6ac3-40ae-afa1-1eb3a5a0ae3d',
            '1bed76ef-cdf1-42f8-bd3e-55d6679f3c92',
            '93e1210e-75bf-4aa9-94db-9f64987bfc5a',
            '4f700fa5-dea3-4a2b-960a-c4bbc2931ca4',
            'c9ce0f64-4c76-40b3-b163-278570f12b8e',
            'a0d38be9-0de6-4ad1-951a-189805bf2b16',
            '0590089c-58ce-4bab-8ed4-169472f86c66'
            );
        
        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET passability_status = 'PASSABLE',
                passability_status_notes = 'Marked as passable by CWF due to upstream spawning observations for Atlantic salmon. This may be at least a partial barrier, undersized and improperly installed (bent).'
            WHERE modelled_id = 'fa057fdd-c88f-4541-ae16-ad49bfdfe706';

    """

    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def matchArchive(connection):

    query = f"""
        WITH matched AS (
            SELECT
            a.id as id,
            nn.id as archive_id,
            nn.dist
            FROM {dbTargetSchema}.{dbBarrierTable} a
            CROSS JOIN LATERAL
            (SELECT
            id,
            ST_Distance(a.snapped_point, b.snapped_point) as dist
            FROM {dbTargetSchema}.{dbBarrierTable}_archive b
            ORDER BY a.snapped_point <-> b.snapped_point
            LIMIT 1) as nn
            WHERE nn.dist < 10
        )

        UPDATE {dbTargetSchema}.{dbBarrierTable} a
            SET id = m.archive_id
            FROM matched m
            WHERE m.id = a.id;

        DROP TABLE {dbTargetSchema}.{dbBarrierTable}_archive;

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

        # print("  matching to archived barrier ids")
        # matchArchive(conn)
        
    print("done")
    
if __name__ == "__main__":
    main()   