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
# ASSUMPTION - data is in equal area projection where distance functions return values in metres
#
import appconfig

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
workingWatershedId = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
watershedTable = appconfig.config['CREATE_LOAD_SCRIPT']['watershed_table']

def main():
    with appconfig.connectdb() as conn:
        
        query = f"""
            CREATE SCHEMA IF NOT EXISTS {dbTargetSchema};
        
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetStreamTable};

            CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{dbTargetStreamTable}(
              {appconfig.dbIdField} uuid not null,
              source_id uuid not null,
              {appconfig.dbWatershedIdField} varchar not null,
              stream_name varchar,
              strahler_order integer,
              segment_length double precision,
              geometry geometry(LineString, {appconfig.dataSrid}),
              primary key ({appconfig.dbIdField})
            );
            
            CREATE INDEX {dbTargetSchema}_{dbTargetStreamTable}_geometry_idx ON {dbTargetSchema}.{dbTargetStreamTable} using gist(geometry);
            
            --ensure results are readable
            GRANT USAGE ON SCHEMA {dbTargetSchema} TO public;
            GRANT SELECT ON {dbTargetSchema}.{dbTargetStreamTable} to public;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {dbTargetSchema} GRANT SELECT ON TABLES TO public;

            INSERT INTO {dbTargetSchema}.{dbTargetStreamTable} 
                ({appconfig.dbIdField}, source_id,
                {appconfig.dbWatershedIdField}, 
                stream_name, strahler_order, geometry)
            SELECT gen_random_uuid(), id,
                aoi_id,
                stream_name, strahler_order,
                CASE
                WHEN ST_WITHIN(t1.geometry,t2.geometry)
                THEN t1.geometry
                ELSE ST_Intersection(t1.geometry, t2.geometry)
                END AS geometry
            FROM {appconfig.dataSchema}.{appconfig.streamTable} t1
            JOIN {appconfig.dataSchema}.{appconfig.watershedTable} t2 ON ST_Intersects(t1.geometry, t2.geometry)
            WHERE aoi_id = '{workingWatershedId}';

            -------------------------
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set segment_length = st_length2d(geometry) / 1000.0;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column geometry_original geometry(LineString, {appconfig.dataSrid});
            update {dbTargetSchema}.{dbTargetStreamTable} set geometry_original = geometry;
            update {dbTargetSchema}.{dbTargetStreamTable} set geometry = st_snaptogrid(geometry, 0.01);
            -------------------------
            
            --TODO: remove this when values are provided
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column {appconfig.streamTableChannelConfinementField} numeric;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column {appconfig.streamTableDischargeField} numeric;
            
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {appconfig.streamTableChannelConfinementField} = floor(random() * 100);
            UPDATE {dbTargetSchema}.{dbTargetStreamTable} set {appconfig.streamTableDischargeField} = floor(random() * 100);
            
       
        """
        # print(query)
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        
    print(f"""Initializing processing for watershed {workingWatershedId} complete.""")

if __name__ == "__main__":
    main()     