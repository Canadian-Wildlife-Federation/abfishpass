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
# This script loads gdb files into postgis database, create by the create_db.py script
#
import subprocess
import appconfig
import zipfile
import tempfile

rawData = appconfig.config['PROCESSING']['fish_observation_data'];

aquaticHabitatFile = "AquaticHabitat.shp"
fishStockingFile = "FishCultureStocking.shp"
fishSurveyFile = "FishSurvey.shp"

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']


#unzip data to temp location

with tempfile.TemporaryDirectory() as workingdir:
    with zipfile.ZipFile(rawData, "r") as zipref:
        zipref.extractall(workingdir)


    with appconfig.connectdb() as conn:
    
        orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    
        toload = [
            ["Loading Aquatic Habitat Data", workingdir + "/" + aquaticHabitatFile, dbTargetSchema + "." + appconfig.config['DATABASE']['aquatic_habitat_table']],
            ["Loading Fish Stocking Data", workingdir + "/" + fishStockingFile, dbTargetSchema + "." + appconfig.config['DATABASE']['fish_stocking_table']],
            ["Loading Fish Survey Data", workingdir + "/" + fishSurveyFile, dbTargetSchema + "." + appconfig.config['DATABASE']['fish_survey_table']],
        ]
    
        for dataset in toload:
            print(dataset[0])
            file = dataset[1]
            datatable = dataset[2]
            
            query = f"""DROP table IF EXISTS {datatable};  """
            #print(query)
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit();
    
            pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + datatable + '" -lco GEOMETRY_NAME=geometry "' + file + '" '
            #print(pycmd)
            subprocess.run(pycmd)
    
    
print("Loading Fish Observation datasets complete")