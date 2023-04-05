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
# This script loads fish habitat information into the database
#
import subprocess
import appconfig

iniSection = appconfig.args.args[0]
streamTable = appconfig.config['DATABASE']['stream_table']
dbTargetSchema = appconfig.config[iniSection]['output_schema']
file = appconfig.config[iniSection]['fish_observation_data']

datatable = dbTargetSchema + ".habitat_data"

def main():

    with appconfig.connectdb() as conn:

        query = f"""DROP TABLE IF EXISTS {datatable};  """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        print("Loading habitat data")
        layer = "habitat"
        orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
        pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + datatable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
        #print(pycmd)
        subprocess.run(pycmd)
        
        query = f"""

        --stream_id will equal source_id in the stream table
        ALTER TABLE {datatable} alter column stream_id type uuid using stream_id::uuid;

        """
        
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    print("Loading fish habitat data complete")

if __name__ == "__main__":
    main()