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

import configparser
import os
import psycopg2 as pg2
import psycopg2.extras
import sys
import enum 

NODATA = -999999

#users can optionally specify a configuration file
configfile = "config.ini"
if (len(sys.argv) > 1):
    configfile = sys.argv[1]
    

config = configparser.ConfigParser()
config.read(configfile)

ogr = config['OGR']['ogr'];
proj = config['OGR']['proj']; 
gdalinfo = config['OGR']['gdalinfo'];
gdalsrsinfo = config['OGR']['gdalsrsinfo'];

dbHost = config['DATABASE']['host'];
dbPort = config['DATABASE']['port'];
dbName = config['DATABASE']['name'];
dbUser = config['DATABASE']['user'];
dbPassword = config['DATABASE']['password'];

dataSchema = config['DATABASE']['data_schema'];
streamTable = config['DATABASE']['stream_table'];
streamTableDischargeField = "discharge";
streamTableChannelConfinementField = "channel_confinement";
fishSpeciesTable = config['DATABASE']['fish_species_table'];

dataSrid = config['DATABASE']['working_srid']  

dbIdField = "id"
dbGeomField = "geometry"
dbWatershedIdField = "watershed_id"

class Accessibility(enum.Enum):
    ACCESSIBLE = 'ACCESSIBLE'
    POTENTIAL = 'POTENTIALLY ACCESSIBLE'
    NOT = 'NOT ACCESSIBLE'


print(f"""--- Configuration Settings Begin ---
Database: {dbHost}:{dbPort}:{dbName}:{dbUser}
ORG: {ogr}
SRID: {dataSrid}
Raw Data Schema: {dataSchema}
--- Configuration Settings End ---
""")   

#if you have multiple version of proj installed
#you might need to set this to match gdal one
#not always required
if (proj != ""):
    os.environ["PROJ_LIB"] = proj

psycopg2.extras.register_uuid()

def connectdb():
    return pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)