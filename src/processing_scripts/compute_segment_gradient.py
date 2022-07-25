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
# Assumes stream network forms a tree where ever node has 0 or 1 out nodes
# Assume - data projection is m length projection or else need to modify how length is computed
# Requires stream name field, in this field a value of UNNAMED represents no-name
#
# In addition to computing vertex and segment gradient it also computes the
# maximum vertex gradient for the stream segment
#
import appconfig

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbSegmentGradientField = appconfig.config['GRADIENT_PROCESSING']['segment_gradient_field']
dbSmoothedGeomField = appconfig.config['ELEVATION_PROCESSING']['smoothedgeometry_field']


def computeSegmentGradient(connection):

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN IF NOT EXISTS {dbSegmentGradientField} double precision;
        
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
        SET {dbSegmentGradientField} = (ST_Z (ST_PointN ({dbSmoothedGeomField}, 1)) - ST_Z (ST_PointN ({dbSmoothedGeomField}, -1))) / ST_Length ({dbSmoothedGeomField})
    """
    #print (query)
    with connection.cursor() as cursor:
        cursor.execute(query)    
            
    connection.commit()



def main():
    #--- main program ---    
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Segment Gradient")
        
        print("  computing vertex gradients")
        computeSegmentGradient(conn)
        
        
    print("done")

if __name__ == "__main__":
    main() 
