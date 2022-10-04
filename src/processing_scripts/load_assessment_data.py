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
# This script loads an assessment data file into the database, joins
# the points to modelled crossings based on a specified buffer distance,
# loads the joined and modelled points to the crossings table,
# and finally loads crossing points to the barriers table.
#
# The script assumes assessment data files only contain data for a single
# HUC 8 watershed.
#

import subprocess
import appconfig

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
rawData = appconfig.config[iniSection]['assessment_data']

dbTempTable = 'assessment_data'
dbTargetTable = appconfig.config['CROSSINGS']['assessed_crossings_table']
dbModelledCrossingsTable = appconfig.config['CROSSINGS']['modelled_crossings_table']
dbCrossingsTable = appconfig.config['CROSSINGS']['crossings_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']

def loadAssessmentData(connection):
        
        # create assessed crossings table
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetTable};

            --TO DO: create the target table format
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        # load assessment data
        orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

        pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + dbTargetSchema + '.' + dbTempTable + '" -lco GEOMETRY_NAME=geometry "' + rawData + '" '
        print(pycmd)
        subprocess.run(pycmd)

        query = f"""
            --TO DO: insert values from dbTempTable to dbTargetTable

        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

def joinAssessmentData(connection)

def loadToBarriers(connection)

#--- main program ---
def main():
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Loading Assessment Data for Stream Crossings")
        
        print("  loading assessment data")
        loadAssessmentData(conn)
        
        print("  joining assessment points to modelled points")
        joinAssessmentData(conn)
        
        print("  adding joined points to crossings and barriers tables")
        loadToBarriers(conn)  
        
    print("done")
    
if __name__ == "__main__":
    main()   