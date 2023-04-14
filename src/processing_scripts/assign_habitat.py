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
# This script computes the habitat models for the
# various fish species 
#

import appconfig
from appconfig import dataSchema

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
habitatTable = dbTargetSchema + ".habitat_data"
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

def computeHabitatModel(connection):

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

            colname = "habitat_" + code

            print("     processing " + name)

            if code == 'as': # atlantic salmon

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = false;

                    --main habitat calculation
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable}
                        SET {colname} = true
                        WHERE segment_gradient <= 0.03;

                    --override some segments as salmon habitat where redds have been observed
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true
                        FROM {habitatTable} f
                        WHERE f.stream_id = source_id
                        AND f.spawning ILIKE '%{code}%';
                    
                    --override some segments that are not salmon habitat based on stream enhancement data
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = false
                        FROM {habitatTable} f
                        WHERE f.stream_id = source_id
                        AND f.not_habitat_{code} IS TRUE;
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()
            
            elif code == 'ae': # american eel

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = false;

                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true
                        WHERE strahler_order >= 2;
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()

            elif code == 'sm': # smelt/gaspereau

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = false;

                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true
                        WHERE {code}_accessibility IN ('{appconfig.Accessibility.ACCESSIBLE.value}', '{appconfig.Accessibility.POTENTIAL.value}');
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()

            else:
                pass

def main():                            
    #--- main program ---    
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Habitat Models Per Species")
        computeHabitatModel(conn)
        
    print("done")


if __name__ == "__main__":
    main()  
