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
updateTable = dbTargetSchema + ".habitat_access_updates"
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
dbSegmentGradientField = appconfig.config['GRADIENT_PROCESSING']['segment_gradient_field']

def computeHabitatModel(connection):
    
    # spawning
    print("Computing spawning habitat")
    query = f"""
        SELECT code, name,
        spawn_gradient_min::float, spawn_gradient_max::float
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
    
        for feature in features:
            code = feature[0]
            name = feature[1]
            mingradient = feature[2]
            maxgradient = feature[3]

            colname = "habitat_spawn_" + code

            print("     processing " + name)

            if code == 'as': # atlantic salmon

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = false;

                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} a
                    SET {colname} = 
                        CASE
                        WHEN b.habitat_spawn_{code} = 'true' THEN true
                        WHEN b.habitat_spawn_{code} = 'false' THEN false
                        ELSE a.{colname} END
                        FROM {updateTable} b
                        WHERE b.stream_id = a.id AND b.habitat_spawn_{code} IS NOT NULL AND b.update_type = 'habitat';
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()
            
            elif code == 'bt': # brook trout

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true;
                    
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
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()

            elif code == 'sm': # smelt

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = false;

                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true
                        WHERE {code}_accessibility IN ('{appconfig.Accessibility.ACCESSIBLE.value}', '{appconfig.Accessibility.POTENTIAL.value}')
                        AND 
                        {dbSegmentGradientField} >= {mingradient} 
                        AND 
                        {dbSegmentGradientField} < {maxgradient};
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()

            else:
                pass

    # rearing
    print("Computing rearing habitat")
    query = f"""
        SELECT code, name,
        rear_gradient_min::float, rear_gradient_max::float
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
    
        for feature in features:
            code = feature[0]
            name = feature[1]
            mingradient = feature[2]
            maxgradient = feature[3]

            colname = "habitat_rear_" + code

            print("     processing " + name)

            if code == 'as': # atlantic salmon

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true;

                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} a
                    SET {colname} = false
                    FROM {updateTable} b
                    WHERE b.stream_id = a.id AND b.habitat_rear_{code} = 'false' AND b.update_type = 'habitat';
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()
            
            elif code == 'bt': # brook trout

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true;
                    
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

            elif code == 'sm': # smelt

                query = f"""
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS {colname};
                    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {colname} boolean;
                    
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = false;

                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                        SET {colname} = true
                        WHERE {code}_accessibility IN ('{appconfig.Accessibility.ACCESSIBLE.value}', '{appconfig.Accessibility.POTENTIAL.value}')
                        AND 
                        {dbSegmentGradientField} >= {mingradient} 
                        AND 
                        {dbSegmentGradientField} < {maxgradient};
                    
                """
                with connection.cursor() as cursor2:
                    cursor2.execute(query)
                connection.commit()

            else:
                pass

    # general habitat
    print("Computing combined habitat")
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
            
            spawning = "habitat_spawn_" + code
            rearing = "habitat_rear_" + code

            colname = "habitat_" + code

            query = f"""
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable}
                    ADD COLUMN IF NOT EXISTS {colname} boolean;
                
                UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                    SET {colname} = false;

                UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                    SET {colname} = CASE WHEN {spawning} = false AND {rearing} = false THEN false ELSE true END;
            
            """
            with connection.cursor() as cursor2:
                cursor2.execute(query)

def main():                            
    #--- main program ---    
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Habitat Models Per Species")
        computeHabitatModel(conn)
        
    print("done")


if __name__ == "__main__":
    main()  
