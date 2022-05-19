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
# snaps barriers to the stream table
# ASSUMPTION - data is in equal area projection where distance functions return values in metres
#
import appconfig

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']

dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

watershed_id = appconfig.config['PROCESSING']['watershed_id']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']


with appconfig.connectdb() as conn:
    
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
    
    #break streams at snapped points
    #todo: may want to ensure this doesn't create small stream segments - ensure barriers are not on top of each other

    query = f"""
    CREATE TEMPORARY TABLE newstreamlines AS
    
    with breakpoints as (
        SELECT a.{appconfig.dbIdField} as id, 
            a.geometry,
            st_collect(st_lineinterpolatepoint(a.geometry, st_linelocatepoint(a.geometry, b.snapped_point))) as rawpnt
        FROM 
            {dbTargetSchema}.{dbTargetStreamTable} a,  
            {dbTargetSchema}.{dbBarrierTable} b 
        WHERE st_distance(st_force2d(a.geometry), b.snapped_point) < 0.000000001 
        GROUP BY a.{appconfig.dbIdField}
    ),
    newlines as (
        SELECT {appconfig.dbIdField},
             st_split(st_snap(geometry, rawpnt, 0.000000001), rawpnt) as geometry
        FROM breakpoints 
    )
    
    SELECT z.{appconfig.dbIdField},
            y.source_id,
            y.{appconfig.dbWatershedIdField},
            y.stream_name,
            y.strahler_order,
            {appconfig.streamTableChannelConfinementField},
            {appconfig.streamTableVelocityField},
            st_geometryn(z.geometry, generate_series(1, st_numgeometries(z.geometry))) as geometry
    FROM newlines z JOIN {dbTargetSchema}.{dbTargetStreamTable} y 
         ON y.{appconfig.dbIdField} = z.{appconfig.dbIdField};
    
    DELETE FROM {dbTargetSchema}.{dbTargetStreamTable} 
    WHERE {appconfig.dbIdField} IN (SELECT {appconfig.dbIdField} FROM newstreamlines);
    
          
    INSERT INTO  {dbTargetSchema}.{dbTargetStreamTable} 
        (id, source_id, {appconfig.dbWatershedIdField}, stream_name, strahler_order, 
        {appconfig.streamTableChannelConfinementField},{appconfig.streamTableVelocityField} , geometry)
    SELECT  uuid_generate_v4(), a.source_id, a.{appconfig.dbWatershedIdField}, 
        a.stream_name, a.strahler_order, a.{appconfig.streamTableChannelConfinementField},
        a.{appconfig.streamTableVelocityField}, a.geometry
    FROM newstreamlines a;
    
    DROP TABLE newstreamlines; 

    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit();

print("Snapping barriers and breaking at stream segements complete.")