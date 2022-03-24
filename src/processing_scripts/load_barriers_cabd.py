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

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
workingWatershedId = appconfig.config['PROCESSING']['watershed_id']
dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']


cabdHost = appconfig.config['CABD_DATABASE']['host'];
cabdPort = appconfig.config['CABD_DATABASE']['port'];
cabdName = appconfig.config['CABD_DATABASE']['name'];
cabdUser = appconfig.config['CABD_DATABASE']['user'];
cabdPassword = appconfig.config['CABD_DATABASE']['password'];


def connectCabd():
    return pg2.connect(database=cabdName,
                   user=cabdUser,
                   host=cabdHost,
                   password=cabdPassword,
                   port=cabdPort)


print(f"""CABD : {cabdHost}:{cabdPort}:{cabdName}:{cabdUser}""")   

with appconfig.connectdb() as conn:
    
    query = f"""
        create table if not exists {dbTargetSchema}.{dbBarrierTable} (
            id uuid not null default uuid_generate_v4(),
            cabd_id uuid,
            original_point geometry(POINT, {appconfig.dataSrid}),
            snapped_point geometry(POINT, {appconfig.dataSrid}),
            name varchar(256),
            type varchar(16),
            primary key (id)
        );
        
        delete from {dbTargetSchema}.{dbBarrierTable} where cabd_id is not null; 
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit();
    
    
    #get bounds of dataset
    query = f"""
        select st_buffer(st_convexhull(st_collect(geometry)), 200) 
        from {dbTargetSchema}.{dbTargetStreamTable}
    """
    
    extentgeom = None
    with conn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchone()
        
        extentgeom = data[0];
        
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
                

print("Loading Barriers from CABD dataset complete")