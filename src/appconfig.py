import configparser
import os
import psycopg2 as pg2
import sys

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

dataSrid = config['DATABASE']['working_srid']  

print(f"""--- Configuration Settings ---
Database: {dbHost}:{dbPort}:{dbName}:{dbUser}
ORG: {ogr}
SRID: {dataSrid}
Raw Data Schema: {dataSchema}
--- End ---
""")   

#if you have multiple version of proj installed
#you might need to set this to match gdal one
#not always required
if (proj != ""):
    os.environ["PROJ_LIB"] = proj


def connectdb():
    return pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)