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
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbSegmentGradientField = appconfig.config['GRADIENT_PROCESSING']['segment_gradient_field']

    
def computeGradientModel(connection):
        
    query = f"""
        SELECT code, name, 
        habitat_gradient_min, habitat_gradient_max
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
            
            print("  processing " + name)
            
            colname = "habitat_gradient_" + code; 
            
            query = f"""
            
                alter table {dbTargetSchema}.{dbTargetStreamTable} 
                    add column if not exists {colname} boolean;
        
                update {dbTargetSchema}.{dbTargetStreamTable} 
                    set {colname} = false;
                
                UPDATE {dbTargetSchema}.{dbTargetStreamTable}
                set {colname} = true
                WHERE
                {code}_accessibility in ( '{appconfig.Accessibility.ACCESSIBLE.value}',
                    '{appconfig.Accessibility.POTENTIAL.value}')
                AND 
                {dbSegmentGradientField} >= {mingradient} 
                AND 
                {dbSegmentGradientField} < {maxgradient}
                
            """
            with connection.cursor() as cursor2:
                cursor2.execute(query)


def computeDischargeModel(connection):
        
    query = f"""
        SELECT code, name, 
        habitat_discharge_min, habitat_discharge_max
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        for feature in features:
            code = feature[0]
            name = feature[1]
            minvelocity = feature[2]
            maxvelocity = feature[3]
            
            print("  processing " + name)
            
            colname = "habitat_discharge_" + code; 
            
            query = f"""
            
                alter table {dbTargetSchema}.{dbTargetStreamTable} 
                    add column if not exists {colname} boolean;
        
                update {dbTargetSchema}.{dbTargetStreamTable} 
                    set {colname} = false;
                
                UPDATE {dbTargetSchema}.{dbTargetStreamTable}
                set {colname} = true
                WHERE
                {code}_accessibility in ( '{appconfig.Accessibility.ACCESSIBLE.value}',
                    '{appconfig.Accessibility.POTENTIAL.value}')
                AND 
                {appconfig.streamTableDischargeField} >= {minvelocity} 
                AND 
                {appconfig.streamTableDischargeField} < {maxvelocity}
                
            """
            with connection.cursor() as cursor2:
                cursor2.execute(query)


def computeConfinementModel(connection):
        
    query = f"""
        SELECT code, name, 
        habitat_channel_confinement_min, habitat_channel_confinement_max
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        for feature in features:
            code = feature[0]
            name = feature[1]
            mincc = feature[2]
            maxcc = feature[3]
            
            print("  processing " + name)
            
            colname = "habitat_channelconfinement_" + code; 
            
            query = f"""
            
                alter table {dbTargetSchema}.{dbTargetStreamTable} 
                    add column if not exists {colname} boolean;
        
                update {dbTargetSchema}.{dbTargetStreamTable} 
                    set {colname} = false;
                
                --TODO: implement model when defined
                UPDATE {dbTargetSchema}.{dbTargetStreamTable}
                set {colname} = true;                
            """
            with connection.cursor() as cursor2:
                cursor2.execute(query)
            
def main():                            
    #--- main program ---    
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Habitat Models Per Species")
        
        print("  computing graident models per species")
        computeGradientModel(conn)
        
        print("  computing discharge models per species")
        computeDischargeModel(conn)
        
        print("  computing channel confinement models per species")
        computeConfinementModel(conn)
        
    print("done")


if __name__ == "__main__":
    main()  
