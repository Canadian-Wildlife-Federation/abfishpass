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
            total_km numeric,
            accessible_all_km numeric,
            potentially_accessible_all_km numeric,
            accessible_spawn_all_km numeric,
            accessible_rear_all_km numeric,
            accessible_habitat_all_km numeric,
            total_spawn_all_km numeric,
            total_rear_all_km numeric,
            total_habitat_all_km numeric,
            connectivity_status numeric
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
                VALUES ({watershed_id});
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
        
            colquery = ''
            accessquery = ''
            allfishaccess = None
            allfishpotentialaccess = None
            allfishaccess_spawn = None
            allfishaccess_rear = None
            allfishaccess_habitat = None
            allfishspawn = None
            allfishrear = None
            allfishhabitat = None
            fishspawnhabitat = ''
            fishrearhabitat = ''
            fishhabitat = ''
            for fish in fishes:        
                
                colquery = f"""
                    {colquery}
                    ALTER TABLE {appconfig.dataSchema}.{statTable}
                    ADD COLUMN IF NOT EXISTS {fish}_accessible_spawn_km numeric,
                    ADD COLUMN IF NOT EXISTS {fish}_potentially_accessible_spawn_km numeric,
                    ADD COLUMN IF NOT EXISTS {fish}_accessible_rear_km numeric,
                    ADD COLUMN IF NOT EXISTS {fish}_potentially_accessible_rear_km numeric,
                    ADD COLUMN IF NOT EXISTS {fish}_total_spawn_km numeric,
                    ADD COLUMN IF NOT EXISTS {fish}_total_rear_km numeric,
                    ADD COLUMN IF NOT EXISTS {fish}_total_habitat_km numeric;
                """
                
                accessquery = f"""
                    {accessquery}            
                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_accessible_spawn_km =
                    (SELECT sum(segment_length)
                    FROM ({alldataquery}) as alldata
                    WHERE alldata.{fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                    AND alldata.habitat_spawn_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';

                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_potentially_accessible_spawn_km =
                    (SELECT sum(segment_length)
                    FROM ({alldataquery}) as alldata
                    WHERE alldata.{fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}'
                    AND alldata.habitat_spawn_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';

                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_accessible_rear_km =
                    (SELECT sum(segment_length)
                    FROM ({alldataquery}) as alldata
                    WHERE alldata.{fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                    AND alldata.habitat_rear_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';

                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_potentially_accessible_rear_km =
                    (SELECT sum(segment_length)
                    FROM ({alldataquery}) as alldata
                    WHERE alldata.{fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}'
                    AND alldata.habitat_rear_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';

                """
                if (allfishaccess is None):
                    allfishaccess = ''
                else:
                    allfishaccess = allfishaccess + " OR "
                allfishaccess = allfishaccess + f"""{fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' """

                if (allfishpotentialaccess is None):
                    allfishpotentialaccess = ''
                else:
                    allfishpotentialaccess = allfishpotentialaccess + " OR "
                allfishpotentialaccess = allfishpotentialaccess + f"""{fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}' """

                
                fishspawnhabitat = f"""
                    {fishspawnhabitat}                
                    WITH alldata AS ({alldataquery})
                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_total_spawn_km =
                    (SELECT sum(segment_length) FROM alldata
                    WHERE habitat_spawn_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';
                """

                fishrearhabitat = f"""
                    {fishrearhabitat}                
                    WITH alldata AS ({alldataquery})
                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_total_rear_km =
                    (SELECT sum(segment_length) FROM alldata
                    WHERE habitat_rear_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';
                """

                fishhabitat = f"""
                    {fishhabitat}                
                    WITH alldata AS ({alldataquery})
                    UPDATE {appconfig.dataSchema}.{statTable} SET {fish}_total_habitat_km =
                    (SELECT sum(segment_length) FROM alldata
                    WHERE habitat_{fish} = true)
                    WHERE watershed_id = '{watershed_id}';
                """

                if (allfishspawn is None):
                    allfishspawn = ''
                else:
                    allfishspawn = allfishspawn + " OR "
                allfishspawn = allfishspawn + f"""habitat_spawn_{fish} = true"""

                if (allfishrear is None):
                    allfishrear = ''
                else:
                    allfishrear = allfishrear + " OR "
                allfishrear = allfishrear + f"""habitat_rear_{fish} = true"""

                if (allfishhabitat is None):
                    allfishhabitat = ''
                else:
                    allfishhabitat = allfishhabitat + " OR "
                allfishhabitat = allfishhabitat + f"""habitat_{fish} = true"""

            allfishaccess_spawn = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET accessible_spawn_all_km =
                (SELECT sum(segment_length) FROM alldata
                WHERE {allfishaccess} AND {allfishspawn})
                WHERE watershed_id = '{watershed_id}';
            """
            
            allfishaccess_rear = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET accessible_rear_all_km =
                (SELECT sum(segment_length) FROM alldata
                WHERE {allfishaccess} AND {allfishrear})
                WHERE watershed_id = '{watershed_id}';
            """

            allfishaccess_habitat = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET accessible_habitat_all_km =
                (SELECT sum(segment_length) FROM alldata
                WHERE {allfishaccess} AND {allfishhabitat})
                WHERE watershed_id = '{watershed_id}';
            """
                
            allfishaccess = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET accessible_all_km =
                (SELECT sum(segment_length) FROM alldata
                WHERE {allfishaccess})
                WHERE watershed_id = '{watershed_id}';
            """

            allfishpotentialaccess = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET potentially_accessible_all_km =
                (SELECT sum(segment_length) FROM alldata
                WHERE {allfishpotentialaccess})
                WHERE watershed_id = '{watershed_id}';
            """
            
            allfishspawn = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET total_spawn_all_km =
                (SELECT sum(segment_length) from alldata WHERE {allfishspawn})
                WHERE watershed_id = '{watershed_id}';
            """

            allfishrear = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET total_rear_all_km =
                (SELECT sum(segment_length) from alldata WHERE {allfishrear})
                WHERE watershed_id = '{watershed_id}';
            """

            allfishhabitat = f"""
                WITH alldata AS ({alldataquery})
                UPDATE {appconfig.dataSchema}.{statTable} SET total_habitat_all_km =
                (SELECT sum(segment_length) from alldata WHERE {allfishhabitat})
                WHERE watershed_id = '{watershed_id}';
            """

            connectivity_status = f"""        
                UPDATE {appconfig.dataSchema}.{statTable} SET connectivity_status =
                (SELECT (accessible_all_km / (accessible_all_km + potentially_accessible_all_km))
                FROM {appconfig.dataSchema}.{statTable}
                WHERE watershed_id = '{watershed_id}')
                WHERE watershed_id = '{watershed_id}';
            """

            query = f"""
                UPDATE {appconfig.dataSchema}.{statTable} SET total_km =
                (SELECT sum(segment_length) FROM ({alldataquery}) as alldata)
                WHERE watershed_id = '{watershed_id}';
                
                {colquery}
                {accessquery}
                {allfishaccess}
                {allfishpotentialaccess}
                {fishspawnhabitat}
                {fishrearhabitat}
                {fishhabitat}
                {allfishaccess_spawn}
                {allfishaccess_rear}
                {allfishaccess_habitat}
                {allfishspawn}
                {allfishrear}
                {allfishhabitat}
                {connectivity_status}
            """
            
            # print(query)
            with connection.cursor() as cursor:
                cursor.execute(query)
    
    print ("Computing Summary Statistics Complete")
    
    
if __name__ == "__main__":
    main()     


