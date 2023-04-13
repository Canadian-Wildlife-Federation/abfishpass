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
# This script summarizes watershed data creating a statistic createTable
# and populating it with various stats 
#
 
import appconfig

wsStreamTable = appconfig.config['PROCESSING']['stream_table']
statTable = "habitat_stats"

def main():
    
    print ("Computing Summary Statistics")
    
    sheds = appconfig.config['HABITAT_STATS']['watershed_data_schemas'].split(",")

    query = f"""
        DROP TABLE IF EXISTS {appconfig.dataSchema}.{statTable};
        
        CREATE TABLE IF NOT EXISTS {appconfig.dataSchema}.{statTable}(
            watershed_id varchar,
            total_km double precision,
            connectivity_status double precision,

            primary key (watershed_id)
        );
    """
    with appconfig.connectdb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()

    alldataquery = None
    for shed in sheds:
        alldataquery = "SELECT * FROM " + shed + "." + wsStreamTable
        watershedidquery = "SELECT DISTINCT watershed_id FROM (" + alldataquery + ") AS alldata"
        
        with appconfig.connectdb() as connection:
            
            with connection.cursor() as cursor:
                cursor.execute(watershedidquery)
                row = cursor.fetchone()
                watershed_id = row[0]
            
            query = f"""
                INSERT INTO {appconfig.dataSchema}.{statTable} (watershed_id)
                VALUES ('{watershed_id}');
            """
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()

            fishes = []

            query = f""" SELECT code FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable}"""
            
            with connection.cursor() as cursor:
                cursor.execute(query)
                
                for row in cursor.fetchall():
                    fishes.append(row[0])
        
            col_query = ''
            fishaccess_query = ''
            fishhabitat_query = ''

            for fish in fishes: 
                
                col_query = f"""
                    {col_query}
                    ALTER TABLE {appconfig.dataSchema}.{statTable}
                    ADD COLUMN IF NOT EXISTS {fish}_accessible_habitat_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_potentially_accessible_habitat_km double precision,
                    ADD COLUMN IF NOT EXISTS {fish}_total_habitat_km double precision;
                """
                
                fishaccess_query = f"""
                    {fishaccess_query}            
                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_accessible_habitat_km =
                    (SELECT sum(segment_length)
                    FROM ({alldataquery}) as alldata
                    WHERE alldata.{fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                    AND alldata.habitat_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';

                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_potentially_accessible_habitat_km =
                    (SELECT sum(segment_length)
                    FROM ({alldataquery}) as alldata
                    WHERE alldata.{fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}'
                    AND alldata.habitat_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';
                """

                fishhabitat_query = f"""
                    {fishhabitat_query}                
                    WITH alldata AS ({alldataquery})
                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_total_habitat_km =
                    (SELECT sum(segment_length) FROM alldata
                    WHERE habitat_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';
                """

            connectivity_status_query = f"""        
                UPDATE {appconfig.dataSchema}.{statTable} SET connectivity_status =
                (SELECT ({fish}_accessible_habitat_km / ({fish}_accessible_habitat_km + {fish}_potentially_accessible_habitat_km))
                FROM {appconfig.dataSchema}.{statTable}
                WHERE watershed_id = '{watershed_id}')
                WHERE watershed_id = '{watershed_id}';
            """

            query = f"""
                UPDATE {appconfig.dataSchema}.{statTable} SET total_km =
                (SELECT sum(segment_length) FROM ({alldataquery}) as alldata)
                WHERE watershed_id = '{watershed_id}';
                
                {col_query}
                {fishaccess_query}
                {fishhabitat_query}
                {connectivity_status_query}
            """
            
            # print(query)
            with connection.cursor() as cursor:
                cursor.execute(query)
    
    print ("Computing Summary Statistics Complete")
    
    
if __name__ == "__main__":
    main()     