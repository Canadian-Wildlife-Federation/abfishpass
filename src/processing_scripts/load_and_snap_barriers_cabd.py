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
# Loads waterfalls and dam barriers from the CABD database into
# local database
#
import appconfig
import psycopg2 as pg2
import psycopg2.extras

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
workingWatershedId = appconfig.config[iniSection]['watershed_id']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']


cabdHost = appconfig.config['CABD_DATABASE']['host'];
cabdPort = appconfig.config['CABD_DATABASE']['port'];
cabdName = appconfig.config['CABD_DATABASE']['name'];
cabdUser = appconfig.config['CABD_DATABASE']['user'];
cabdPassword = appconfig.config['CABD_DATABASE']['password'];
cabdBuffer = appconfig.config['CABD_DATABASE']['buffer'];

def connectCabd():
    return pg2.connect(database=cabdName,
                   user=cabdUser,
                   host=cabdHost,
                   password=cabdPassword,
                   port=cabdPort)


def main():
    print(f"""CABD : {cabdHost}:{cabdPort}:{cabdName}:{cabdUser}""")   
    
    with appconfig.connectdb() as conn:
        #creates barriers table with attributes from CABD and crossings table
        query = f"""
            create table if not exists {dbTargetSchema}.{dbBarrierTable} (
                id uuid not null default uuid_generate_v4(),
                cabd_id uuid,
                modelled_id uuid,
                assessment_id varchar,
                original_point geometry(POINT, {appconfig.dataSrid}),
                snapped_point geometry(POINT, {appconfig.dataSrid}),
                name varchar(256),
                type varchar(32),

                dam_name_en varchar(512),
                dam_owner varchar(512),
                use_code smallint,

                disp_num varchar,
                stream_name varchar,
                strahler_order integer,
                stream_id uuid,
                stream_measure numeric,
                wshed_name varchar,
                wshed_priority varchar,
              --feature_name varchar,
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

                primary key (id)
            );
            
            delete from {dbTargetSchema}.{dbBarrierTable} where cabd_id is not null; 
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit();
        
        
        #get bounds of dataset
        query = f"""
            select st_buffer(st_convexhull(st_collect(geometry)), {cabdBuffer}) 
            from {dbTargetSchema}.{dbTargetStreamTable}
        """
        
        extentgeom = None
        with conn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchone()
            
            extentgeom = data[0];
            
        #TO DO: Get additional fields from CABD - dam owner, dam use_code
        barriers = (("Dams", "dams.dams", "dam", "case when dam_name_en is not null then dam_name_en else dam_name_fr end"),
                    ("Waterfalls", "waterfalls.waterfalls", "waterfall", "case when fall_name_en is not null then fall_name_en else fall_name_fr end")
                    )
    
        insertquery = f"""
                insert into {dbTargetSchema}.{dbBarrierTable} (cabd_id, original_point, name, type)
                values (%s, %s, %s, %s);
        """
            
        for dataset in barriers:
            print(f"""Loading {dataset[0]}""")
        
            query = f"""
                with boundary as (
                    select st_transform (%s::geometry, 4617) as geom
                )
                select 
                  cabd_id, 
                  {dataset[3]},
                  st_transform(original_point, {appconfig.dataSrid})
                from {dataset[1]}, boundary
                where st_intersects(original_point, boundary.geom) 
            """
    
            newdata = []
         
            with connectCabd() as cabdconn:
                
                with cabdconn.cursor() as cabdcursor:
                    cabdcursor.execute(query, (extentgeom,))
                    for record in cabdcursor:
                        newdata.append((record[0], record[2], record[1], dataset[2] ))
                
                
            with conn.cursor() as cursor:    
                psycopg2.extras.execute_batch(cursor, insertquery, newdata);
                
                conn.commit()
                        
        #snaps barrier features to network
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
           
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit();           
         
    print("Loading Barriers from CABD dataset complete")

if __name__ == "__main__":
    main()     