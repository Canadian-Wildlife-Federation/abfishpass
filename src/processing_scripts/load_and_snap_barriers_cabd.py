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
# Loads dam barriers from the CABD API into local database
#
import appconfig
import psycopg2 as pg2
import psycopg2.extras
import json, urllib.request, requests

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
workingWatershedId = appconfig.config[iniSection]['watershed_id']
nhnWatershedId = appconfig.config[iniSection]['nhn_watershed_id']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

def main():
    
    with appconfig.connectdb() as conn:
        # creates barriers table with attributes from CABD and crossings table
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbBarrierTable};

            create table if not exists {dbTargetSchema}.{dbBarrierTable} (
                id uuid not null default uuid_generate_v4(),
                cabd_id uuid,
                modelled_id uuid,
                assessment_id varchar,
                original_point geometry(POINT, {appconfig.dataSrid}),
                snapped_point geometry(POINT, {appconfig.dataSrid}),
                name varchar(256),
                type varchar(32),
                owner varchar,
                passability_status varchar,

                dam_use varchar,

                disp_num varchar,
                stream_name varchar,
                strahler_order integer,
                stream_id uuid,
                stream_measure numeric,
                wshed_name varchar,
                wshed_priority varchar,
                transport_feature_name varchar,

                species_upstr varchar[],
                species_downstr varchar[],
                stock_upstr varchar[],
                stock_downstr varchar[],

                barriers_upstr varchar[],
                barriers_downstr varchar[],
                barrier_cnt_upstr integer,
                barrier_cnt_downstr integer,
                
                critical_habitat varchar[],
                
                last_inspection date,
                crossing_status varchar CHECK (crossing_status in ('MODELLED', 'ASSESSED', 'HABITAT_CONFIRMATION', 'DESIGN', 'REMEDIATED')),
                crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
                crossing_type varchar,
                crossing_subtype varchar,
                
                habitat_quality varchar,
                year_planned integer,
                year_complete integer,
                comments varchar,

                primary key (id)
            );
            
            delete from {dbTargetSchema}.{dbBarrierTable} where cabd_id is not null; 
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # retrieve barrier data from CABD API
        url = f"https://cabd-web.azurewebsites.net/cabd-api/features/dams?&filter=nhn_watershed_id:eq:{nhnWatershedId}"
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())

        feature_data = data["features"]
        output_data = []

        for feature in feature_data:
            output_feature = []
            output_feature.append(feature["properties"]["cabd_id"])
            output_feature.append(feature["geometry"]["coordinates"][0])
            output_feature.append(feature["geometry"]["coordinates"][1])
            output_feature.append(feature["properties"]["dam_name_en"])
            output_feature.append(feature["properties"]["owner"])
            output_feature.append(feature["properties"]["dam_use"])
            output_feature.append(feature["properties"]["passability_status"])
            output_data.append(output_feature)

        insertquery = f"""
            INSERT INTO {dbTargetSchema}.{dbBarrierTable} (
                cabd_id, 
                original_point,
                name,
                owner,
                dam_use,
                passability_status,
                type)
            VALUES (%s, ST_Transform(ST_GeomFromText('POINT(%s %s)',4617),{appconfig.dataSrid}), %s, %s, %s, UPPER(%s), 'dam');
        """
        with conn.cursor() as cursor:
            for feature in output_data:
                cursor.execute(insertquery, feature)
        conn.commit()
                        
        # snaps barrier features to network
        query = f"""
            CREATE OR REPLACE FUNCTION public.snap_to_network(src_schema varchar, src_table varchar, raw_geom varchar, snapped_geom varchar, max_distance_m double precision) RETURNS VOID AS $$
            DECLARE    
              pnt_rec RECORD;
              fp_rec RECORD;
            BEGIN
                FOR pnt_rec IN EXECUTE format('SELECT id, %I as rawg FROM %I.%I WHERE %I is not null', raw_geom, src_schema, src_table,raw_geom) 
                LOOP 
                    FOR fp_rec IN EXECUTE format ('SELECT fp.geometry  as geometry, st_distance(%L::geometry, fp.geometry) AS distance FROM {dbTargetSchema}.{dbTargetStreamTable} fp WHERE st_expand(%L::geometry, %L) && fp.geometry and st_distance(%L::geometry, fp.geometry) < %L ORDER BY distance ', pnt_rec.rawg, pnt_rec.rawg, max_distance_m, pnt_rec.rawg, max_distance_m)
                    LOOP
                        EXECUTE format('UPDATE %I.%I SET %I = ST_LineInterpolatePoint(%L::geometry, ST_LineLocatePoint(%L::geometry, %L::geometry) ) WHERE id = %L', src_schema, src_table, snapped_geom,fp_rec.geometry, fp_rec.geometry, pnt_rec.rawg, pnt_rec.id);
                        EXIT;
                    END LOOP;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
         
            SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');

            --remove any dam features not snapped to streams
            --because using nhn_watershed_id can cover multiple HUC8 watersheds
            DELETE FROM {dbTargetSchema}.{dbBarrierTable}
            WHERE snapped_point IS NULL
            AND type = 'dam'; 
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
         
    print("Loading Barriers from CABD dataset complete")

if __name__ == "__main__":
    main()     