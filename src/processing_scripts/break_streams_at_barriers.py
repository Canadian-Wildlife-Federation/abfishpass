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
from imagecodecs.imagecodecs import NONE

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']
dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']
dbCrossingsTable = appconfig.config['CROSSINGS']['crossings_table']
dbVertexTable = appconfig.config['GRADIENT_PROCESSING']['vertex_gradient_table']
dbTargetGeom = appconfig.config['ELEVATION_PROCESSING']['smoothedgeometry_field']

def breakstreams (conn):
        
    #find all break points
    # all barriers regardless of passability status (dams, modelled crossings, and assessed crossings)
    # all gradient barriers (Vertex gradient > min fish gradient)
    #  -> these are a bit special as we only want to break once for
    #     a segment if vertex gradient continuously large 
    
    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.break_points;
            
        CREATE TABLE {dbTargetSchema}.break_points(
            point geometry(POINT, {appconfig.dataSrid}),
            barrier_id integer,
            type varchar,
            passability_status varchar
            );
    
        -- barriers
        INSERT INTO {dbTargetSchema}.break_points(point, barrier_id, type, passability_status) 
            SELECT snapped_point, id, type, passability_status
            FROM {dbTargetSchema}.{dbBarrierTable};
    """
        
    #print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
        
    # break at gradient points

    query = f"""
        SELECT min(accessibility_gradient) as minvalue 
        FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable}
    """
    
    mingradient = -1
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        mingradient = features[0][0]
        
        
    query = f"""    
        SELECT mainstem_id, st_Force2d(vertex_pnt), gradient
        FROM {dbTargetSchema}.{dbVertexTable}
        ORDER BY mainstem_id, downstream_route_measure
    """
        
    with conn.cursor() as cursor:
        cursor.execute(query)
        
        features = cursor.fetchall()
        
        lastmainstem = NONE
        lastgradient = -1
        
        for feature in features:
            mainstem = feature[0]
            point = feature[1]
            gradient = feature[2]
            
            insert = False
            if (lastmainstem != mainstem and gradient > mingradient):
                #we need to find what the gradient is at the downstream point here
                # and only add this as a break point
                # if downstream vertex is < 0.15
                query = f"""
                
                    with pnt as (
                        SELECT st_endpoint(a.geometry) as endpnt
                        FROM {dbTargetSchema}.{dbTargetStreamTable} a
                        WHERE st_intersects( a.geometry, '{point}')
                    )
                    SELECT gradient
                    FROM {dbTargetSchema}.{dbVertexTable} a, pnt
                    WHERE a.vertex_pnt && pnt.endpnt
                    AND gradient <= {mingradient}
                """ 
                #print(query)
                with conn.cursor() as cursor3:
                    cursor3.execute(query)
                    features3 = cursor3.fetchall()
                    if (len(features3) > 0):
                        insert = True
    
                

            elif (gradient > mingradient) and \
                not((lastgradient > mingradient and gradient > mingradient and lastmainstem == mainstem)):
                
                insert = True
            
            if insert:
                # this is a point that is not the first point on a new mainstem 
                # has a gradient larger than required values
                # and has a downstream gradient that is less than required values  
                query = f"""INSERT INTO {dbTargetSchema}.break_points(point, barrier_id, type, passability_status) values ('{point}', nextval('{dbTargetSchema}.{dbBarrierTable}_id_seq'), 'gradient_barrier', 'BARRIER');""" 
                with conn.cursor() as cursor2:
                    cursor2.execute(query)
            lastmainstem = mainstem
            lastgradient = gradient
            
    #break streams at snapped points
    #todo: may want to ensure this doesn't create small stream segments - 
    #ensure barriers are not on top of each other
    conn.commit()
    print("breaking streams")
    
    query = f"""
        CREATE TEMPORARY TABLE newstreamlines AS
        
        with breakpoints as (
            SELECT a.{appconfig.dbIdField} as id, 
                a.geometry,
                st_collect(st_lineinterpolatepoint(a.geometry, st_linelocatepoint(a.geometry, b.point))) as rawpnt
            FROM 
                {dbTargetSchema}.{dbTargetStreamTable} a,  
                {dbTargetSchema}.break_points b 
            WHERE st_distance(st_force2d(a.geometry_smoothed3d), b.point) < 0.000000001 
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
                {appconfig.streamTableDischargeField},
                y.mainstem_id,
                st_geometryn(z.geometry, generate_series(1, st_numgeometries(z.geometry))) as geometry
        FROM newlines z JOIN {dbTargetSchema}.{dbTargetStreamTable} y 
             ON y.{appconfig.dbIdField} = z.{appconfig.dbIdField};
        
        DELETE FROM {dbTargetSchema}.{dbTargetStreamTable} 
        WHERE {appconfig.dbIdField} IN (SELECT {appconfig.dbIdField} FROM newstreamlines);
        
              
        INSERT INTO  {dbTargetSchema}.{dbTargetStreamTable} 
            (id, source_id, {appconfig.dbWatershedIdField}, stream_name, strahler_order, 
            segment_length,
            {appconfig.streamTableChannelConfinementField},{appconfig.streamTableDischargeField},
            mainstem_id, geometry)
        SELECT gen_random_uuid(), a.source_id, a.{appconfig.dbWatershedIdField}, 
            a.stream_name, a.strahler_order,
            st_length2d(a.geometry) / 1000.0, 
            a.{appconfig.streamTableChannelConfinementField},
            a.{appconfig.streamTableDischargeField}, 
            mainstem_id, a.geometry
        FROM newstreamlines a;

        DROP INDEX IF EXISTS {dbTargetSchema}."smooth_geom_idx";
        CREATE INDEX smooth_geom_idx ON {dbTargetSchema}.{dbTargetStreamTable} USING gist({dbTargetGeom});
        
        DROP TABLE newstreamlines;
    
    """
        
    #print(query)
        
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def recomputeMainstreamMeasure(connection):
    
    query = f"""
        WITH mainstems AS (
            SELECT st_reverse(st_linemerge(st_collect(geometry))) as geometry, mainstem_id
            FROM {dbTargetSchema}.{dbTargetStreamTable}
            GROUP BY mainstem_id
        ),
        measures AS (
            SELECT 
                (st_linelocatepoint ( b.geometry, st_startpoint(a.geometry)) * st_length(b.geometry)) / 1000.0 as startpct, 
                (st_linelocatepoint ( b.geometry, st_endpoint(a.geometry)) * st_length(b.geometry))  / 1000.0 as endpct,
                a.id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a, mainstems b
            WHERE a.mainstem_id = b.mainstem_id
        )
        UPDATE {dbTargetSchema}.{dbTargetStreamTable}
        SET downstream_route_measure = measures.endpct, 
            upstream_route_measure = measures.startpct
        FROM measures
        WHERE measures.id = {dbTargetSchema}.{dbTargetStreamTable}.id
    """
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)

def updateBarrier(connection):
    
    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS stream_id;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS stream_id_up uuid;
        
        UPDATE {dbTargetSchema}.{dbBarrierTable} SET stream_id_up = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbBarrierTable} b
            WHERE a.geometry && st_buffer(b.snapped_point, 0.0000001) and
                st_intersects(st_endpoint(a.geometry), st_buffer(b.snapped_point, 0.0000001))
        )
        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET stream_id_up = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbBarrierTable}.id;
            
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN IF NOT EXISTS stream_id_down uuid;

        UPDATE {dbTargetSchema}.{dbBarrierTable} SET stream_id_down = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbBarrierTable} b
            WHERE a.geometry && st_buffer(b.snapped_point, 0.0000001) and
                st_intersects(st_startpoint(a.geometry), st_buffer(b.snapped_point, 0.0000001))
        )
        UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET stream_id_down = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbBarrierTable}.id;
        
        --update crossing table with same info for consistency
        
        ALTER TABLE {dbTargetSchema}.{dbModelledCrossingsTable} DROP COLUMN IF EXISTS stream_id;

        ALTER TABLE {dbTargetSchema}.{dbCrossingsTable} DROP COLUMN IF EXISTS stream_id;
        ALTER TABLE {dbTargetSchema}.{dbCrossingsTable} ADD COLUMN IF NOT EXISTS stream_id_up uuid;
        ALTER TABLE {dbTargetSchema}.{dbCrossingsTable} ADD COLUMN IF NOT EXISTS stream_id_down uuid;
        UPDATE {dbTargetSchema}.{dbCrossingsTable} SET stream_id_up = null;
        UPDATE {dbTargetSchema}.{dbCrossingsTable} SET stream_id_down = null;

        UPDATE {dbTargetSchema}.{dbCrossingsTable} AS a
            SET 
            stream_id_up = (SELECT stream_id_up FROM {dbTargetSchema}.{dbBarrierTable} AS b WHERE a.modelled_id = b.modelled_id),
            stream_id_down = (SELECT stream_id_down FROM {dbTargetSchema}.{dbBarrierTable} AS b WHERE a.modelled_id = b.modelled_id);

    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)                    
                        
def main():
    with appconfig.connectdb() as connection:
        print("    breaking streams at barrier points")
        breakstreams(connection)
        
        print("    recomputing mainstem measures")
        recomputeMainstreamMeasure(connection)
    
        print("    updating barrier stream references")
        updateBarrier(connection)
    
    print("Breaking stream complete.")
    
if __name__ == "__main__":
    main()     