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
# This script creates the fish_species table containing accessibility and 
# habitat parameters for species of interest from a CSV specified by the user
#

import appconfig
import subprocess
import sys

dataFile = appconfig.config['DATABASE']['fish_parameters']
sourceTable = appconfig.dataSchema + ".fish_species_raw"

def main():
    with appconfig.connectdb() as conn:

        query = f"""
            DROP TABLE IF EXISTS {sourceTable};
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # load data using ogr
        orgDb = "dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost +"' port='"+ appconfig.dbPort + "' user='" + appconfig.dbUser + "' password='" + appconfig.dbPassword + "'"
        pycmd = '"' + appconfig.ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" "' + dataFile + '"' + ' -nln "' + sourceTable + '" -oo AUTODETECT_TYPE=YES -oo EMPTY_STRING_AS_NULL=YES'
        print(pycmd)
        subprocess.run(pycmd)
        print("CSV loaded to table: " + sourceTable)

        query = f"""
            DROP TABLE IF EXISTS {appconfig.dataSchema}.{appconfig.fishSpeciesTable};

            CREATE TABLE {appconfig.dataSchema}.{appconfig.fishSpeciesTable}(
                code varchar(4) PRIMARY KEY,
                name varchar,
                allcodes varchar[],
                
                accessibility_gradient double precision not null,
                
                spawn_gradient_min numeric,
                spawn_gradient_max numeric,
                rear_gradient_min numeric,
                rear_gradient_max numeric,
                
                spawn_discharge_min numeric,
                spawn_discharge_max numeric,
                rear_discharge_min numeric,
                rear_discharge_max numeric,
                
                spawn_channel_confinement_min numeric,
                spawn_channel_confinement_max numeric,
                rear_channel_confinement_min numeric,
                rear_channel_confinement_max numeric
                );
            """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        query = f"""
            INSERT INTO {appconfig.dataSchema}.{appconfig.fishSpeciesTable}(
                code,
                name,
                allcodes,

                accessibility_gradient,

                spawn_gradient_min,
                spawn_gradient_max,
                rear_gradient_min,
                rear_gradient_max,
                
                spawn_discharge_min,
                spawn_discharge_max,
                rear_discharge_min,
                rear_discharge_max,
                
                spawn_channel_confinement_min,
                spawn_channel_confinement_max,
                rear_channel_confinement_min,
                rear_channel_confinement_max
            )
            SELECT
                code,
                name,
                string_to_array(trim(both '"' from allcodes), ','),

                accessibility_gradient,

                spawn_gradient_min,
                spawn_gradient_max,
                rear_gradient_min,
                rear_gradient_max,
                
                spawn_discharge_min,
                spawn_discharge_max,
                rear_discharge_min,
                rear_discharge_max,
                
                spawn_channel_confinement_min,
                spawn_channel_confinement_max,
                rear_channel_confinement_min,
                rear_channel_confinement_max
            FROM {sourceTable};

            DROP TABLE {sourceTable};
            """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    print (f"""Species parameters loaded to {appconfig.dataSchema}.{appconfig.fishSpeciesTable}""")

if __name__ == "__main__":
    main()