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
# For each segment computes the maximum downstream gradient then uses
# this and the barrier information to compute species accessibility
# for each fish species
#


import appconfig
from appconfig import dataSchema

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
    
def computeAccessibility(connection):
        
    query = f"""
        SELECT code, name
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        for feature in features:
            code = feature[0]
            name = feature[1]

            if code == 'ae': # american eel

                print("  processing " + name)
                
                query = f"""
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {code}_accessibility;
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN {code}_accessibility varchar;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                    SET {code}_accessibility = 
                    CASE 
                    WHEN (gradient_barrier_down_cnt = 0 and barrier_down_cnt = 0) THEN '{appconfig.Accessibility.ACCESSIBLE.value}'
                    WHEN (gradient_barrier_down_cnt = 0 and barrier_down_cnt > 0) THEN '{appconfig.Accessibility.POTENTIAL.value}'
                    ELSE '{appconfig.Accessibility.NOT.value}' END;
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                
                connection.commit()

            else:

                print("  processing " + name)
                
                query = f"""
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {code}_accessibility;
                
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN {code}_accessibility varchar;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                    SET {code}_accessibility = 
                    CASE 
                    WHEN (gradient_barrier_down_cnt = 0 and barrier_down_cnt = 0) THEN '{appconfig.Accessibility.ACCESSIBLE.value}'
                    WHEN (gradient_barrier_down_cnt = 0 and barrier_down_cnt > 0) THEN '{appconfig.Accessibility.POTENTIAL.value}'
                    ELSE '{appconfig.Accessibility.NOT.value}' END;
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                
                connection.commit()

def main():        
    #--- main program ---
            
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Gradient Accessibility Per Species")
        computeAccessibility(conn)
        
    print("done")

    
if __name__ == "__main__":
    main() 
