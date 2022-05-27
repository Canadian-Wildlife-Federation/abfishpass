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
# This script loads fish inventory files into the database and snaps
# the points to the stream network computing the stream id and stream measure
# of the snapped point
#
import subprocess
import appconfig
import zipfile
import tempfile

rawData = appconfig.config['PROCESSING']['fish_observation_data'];

aquaticHabitatFile = "AquaticHabitat.shp"
fishStockingFile = "FishCultureStocking.shp"
fishSurveyFile = "FishSurvey.shp"

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

#unzip data to temp location
def main():
    with tempfile.TemporaryDirectory() as workingdir:
        with zipfile.ZipFile(rawData, "r") as zipref:
            zipref.extractall(workingdir)
    
    
        with appconfig.connectdb() as conn:
        
            orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
        
            toload = [
                ["Loading Aquatic Habitat Data", workingdir + "/" + aquaticHabitatFile, dbTargetSchema, appconfig.config['DATABASE']['aquatic_habitat_table']],
                ["Loading Fish Stocking Data", workingdir + "/" + fishStockingFile, dbTargetSchema, appconfig.config['DATABASE']['fish_stocking_table']],
                ["Loading Fish Survey Data", workingdir + "/" + fishSurveyFile, dbTargetSchema,  appconfig.config['DATABASE']['fish_survey_table']],
            ]
        
            for dataset in toload:
                print(dataset[0])
                file = dataset[1]
                dataschema = dataset[2]
                datatablename = dataset[3]
                
                query = f"""DROP table IF EXISTS {dataschema}.{datatablename};  """
                #print(query)
                with conn.cursor() as cursor:
                    cursor.execute(query)
                conn.commit();
        
                pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + dataschema + '.' + datatablename + '" -lco GEOMETRY_NAME=geometry "' + file + '" '
                #print(pycmd)
                subprocess.run(pycmd)
            
                #snap to flowpath
                
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
                    
                    ALTER TABLE {dataschema}.{datatablename} add column id uuid not null default uuid_generate_v4();
                    
                    ALTER TABLE {dataschema}.{datatablename} add column snapped_point geometry(POINT, {appconfig.dataSrid});
                    
                    SELECT public.snap_to_network('{dataschema}', '{datatablename}', 'geometry', 'snapped_point', '{snapDistance}');
                    
                    ALTER TABLE {dataschema}.{datatablename} add column stream_id uuid;
                    ALTER TABLE {dataschema}.{datatablename} add column stream_measure numeric;
                    
                    with match as (
                    SELECT a.id as stream_id, b.id as pntid, st_linelocatepoint(a.geometry, b.snapped_point) as streammeasure
                    FROM {dbTargetSchema}.{dbTargetStreamTable} a, {dataschema}.{datatablename} b
                    WHERE st_intersects(a.geometry, st_buffer(b.snapped_point, 0.0001))
                    )
                    UPDATE {dataschema}.{datatablename}
                    SET stream_id = a.stream_id, stream_measure = a.streammeasure
                    FROM match a WHERE a.pntid = {dataschema}.{datatablename}.id;
    
                """
                with conn.cursor() as cursor:
                    cursor.execute(query)
                conn.commit();
    print("Loading Fish Observation datasets complete")
    
if __name__ == "__main__":
    main()     