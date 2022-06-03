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
    
    alldataquery = None
    for shed in sheds:
        if (alldataquery is not None):
            alldataquery = alldataquery + " UNION "
        else:
            alldataquery = ''    
        alldataquery = alldataquery + " SELECT * FROM " + shed + "." + wsStreamTable
    
    with appconfig.connectdb() as connection:

        fishes = []
        
        query = f""" SELECT code FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable}"""
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            
            for row in cursor.fetchall():
                fishes.append(row[0])


        accessquery = ''
        allfishaccess = None
        allfishhabitat = None
        fishhabitat = ''
        for fish in fishes:        
            accessquery = f"""
                {accessquery}                
                INSERT INTO {appconfig.dataSchema}.{statTable}(stat, value)
                SELECT '{fish} ' || {fish}_accessibility, sum(segment_length)
                FROM ({alldataquery}) as alldata
                GROUP BY {fish}_accessibility;
            """
            if (allfishaccess is None):
                allfishaccess = ''
            else:
                allfishaccess = allfishaccess + " OR "
            allfishaccess = allfishaccess + f"""{fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' """

            fishhabitat = f"""
                {fishhabitat}                
                WITH alldata AS ({alldataquery})
                INSERT INTO {appconfig.dataSchema}.{statTable}(stat, value)
                SELECT 
                    CASE WHEN habitat_gradient_{fish} 
                        AND habitat_discharge_{fish}
                        AND habitat_channelconfinement_{fish}
                    THEN  '{fish} fish habitat'
                    ELSE '{fish} not fish habitat'
                    END as ishabitat, sum(segment_length)
                FROM alldata
                GROUP BY ishabitat;
            """
            
            if (allfishhabitat is None):
                allfishhabitat = ''
            else:
                allfishhabitat = allfishhabitat + " OR "
            allfishhabitat = allfishhabitat + f"""(habitat_gradient_{fish} AND habitat_discharge_{fish} AND habitat_channelconfinement_{fish}) """
            
        allfishaccess = f"""
            WITH alldata AS ({alldataquery})
            INSERT INTO {appconfig.dataSchema}.{statTable}(stat, value)
            SELECT 
                CASE 
                    WHEN {allfishaccess} 
                    THEN  'all accessible' 
                    ELSE 'all not accessible' 
                    END as isaccess, sum(segment_length)
            FROM alldata
            GROUP BY isaccess;
        """
        
        allfishhabitat = f"""
        WITH alldata AS ({alldataquery})
                INSERT INTO {appconfig.dataSchema}.{statTable}(stat, value)
                SELECT 
                    CASE WHEN {allfishhabitat}
                    THEN  'any fish habitat'
                    ELSE 'not any fish habitat'
                    END as ishabitat, sum(segment_length)
                FROM alldata
                GROUP BY ishabitat;
        """
        

        query = f"""
            DROP TABLE IF EXISTS {appconfig.dataSchema}.{statTable};
            
            CREATE TABLE {appconfig.dataSchema}.{statTable}(
                stat varchar,
                value float
            );
            
            INSERT INTO {appconfig.dataSchema}.{statTable}(stat, value)
            SELECT 'Total Stream Length (km)', sum(segment_length) FROM ({alldataquery}) as alldata;
            
            {accessquery}
            {allfishaccess}
            {fishhabitat}
            {allfishhabitat}
        """
        
        #print (query)
        with connection.cursor() as cursor:
            cursor.execute(query)
    
    print ("Computing Summary Statistics Complete")
    
    
if __name__ == "__main__":
    main()     


