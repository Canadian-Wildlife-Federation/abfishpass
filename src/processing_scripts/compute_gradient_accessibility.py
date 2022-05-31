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
# For each segment computes the maximum downstream gradient then uses
# this and the barrier information to compute species accessibility
# for each fish species
#


import appconfig
import shapely.wkb
from collections import deque
import psycopg2.extras
from appconfig import dataSchema

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
watershed_id = appconfig.config['PROCESSING']['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbMaxDownGradientField = appconfig.config['GRADIENT_PROCESSING']['max_downstream_gradient_field']
dbSegmentGradientField = appconfig.config['GRADIENT_PROCESSING']['segment_gradient_field']


edges = []
nodes = dict()

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        
    def addInEdge(self, edge):
        self.inedges.append(edge)
   
    def addOutEdge(self, edge):
        self.outedges.append(edge)
    
    def getMinDownstreamGradient(self):
        if (len(self.outedges) == 0):
            return 0;
        
        minvalue = None;
        for edge in self.outedges:
            if (edge.mindowngraident < 0 ): 
                return -1
            elif (minvalue is None) or (edge.mindowngraident < minvalue):
                minvalue = edge.mindowngraident
        
        return minvalue;
    
class Edge:
    def __init__(self, fromnode, tonode, fid, maxgradient, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.fid = fid
        self.visited = False
        self.maxgradient = maxgradient
        self.mindowngraident = -1

        
def createNetwork(connection):
    query = f"""
        SELECT a.{appconfig.dbIdField} as id, 
            {dbSegmentGradientField}, a.{appconfig.dbGeomField}
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        for feature in features:
            fid = feature[0]
            maxgradient = feature[1]
            geom = shapely.wkb.loads(feature[2] , hex=True)
            
            startc = geom.coords[0]
            endc = geom.coords[len(geom.coords)-1]
            
            startt = (startc[0], startc[1])
            endt = (endc[0], endc[1])            
            
            if (startt in nodes.keys()):
                fromNode = nodes[startt]
            else:
                #create new node
                fromNode = Node(startc[0], startc[1])
                nodes[startt] = fromNode
            
            if (endt in nodes.keys()):
                toNode = nodes[endt]
            else:
                #create new node
                toNode = Node(endc[0], endc[1])
                nodes[endt] = toNode
            
            edge = Edge(fromNode, toNode, fid, maxgradient, geom)
            edges.append(edge)
            
            fromNode.addOutEdge(edge)
            toNode.addInEdge(edge)     
            


def processNodes():

    #walk up 
    toprocess = deque()
    for node in nodes.values():
        if (len(node.outedges) == 0):
            toprocess.append(node)
    
    while (toprocess):
        node = toprocess.popleft()
        
        downg = node.getMinDownstreamGradient();
        
        if (downg < 0):
            toprocess.append(node)
            continue;
        
        for edge in node.inedges:
            edge.mindowngraident = max (edge.maxgradient, downg)
            toprocess.append(edge.fromNode)
        

def writeResults(connection):
      
    updatequery = f"""
        UPDATE {dbTargetSchema}.{dbTargetStreamTable}
        SET {dbMaxDownGradientField} = %s
        WHERE {appconfig.dbIdField} = %s;
    """
    
    newdata = []
    
    for edge in edges:
        newdata.append( (edge.mindowngraident, edge.fid) )
    
    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata);
            
    connection.commit()
    
def computeAccessibility(connection):
        
    query = f"""
        SELECT code, name, accessibility_gradient, allcodes
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        for feature in features:
            code = feature[0]
            name = feature[1]
            maxvalue = feature[2]
            
            allcodes = feature[3]
            
            fishop = ""
            for fcode in allcodes:
                fishop = fishop + "upper('" + fcode + "') like ANY (fish_stock || fish_survey || fish_stock_up || fish_survey_up) or " 
                
                
            print("  processing " + name)
            
            query = f"""
            
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} drop column if exists {code}_accessibility;
            
                ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} add column {code}_accessibility varchar;
                
                UPDATE {dbTargetSchema}.{dbTargetStreamTable} 
                set {code}_accessibility = 
                CASE 
                  WHEN {fishop} ({dbMaxDownGradientField} < {maxvalue} and barrier_down_cnt = 0) THEN '{appconfig.Accessibility.ACCESSIBLE.value}'
                  WHEN {dbMaxDownGradientField} < {maxvalue} and barrier_down_cnt > 0 THEN '{appconfig.Accessibility.POTENTIAL.value}'
                  ELSE '{appconfig.Accessibility.NOT.value}' END;
                
            """
            with connection.cursor() as cursor2:
                cursor2.execute(query)

def main():        
    #--- main program ---
    
    edges.clear()
    nodes.clear()
            
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Gradient Accessability Per Species")
        print("  creating output column")
        #add a new geometry column for output removing existing one
        query = f"""
            alter table {dbTargetSchema}.{dbTargetStreamTable} 
                add column if not exists {dbMaxDownGradientField} double precision;
            
            update {dbTargetSchema}.{dbTargetStreamTable} set {dbMaxDownGradientField} = null;
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            
        print("  creating network")
        createNetwork(conn)
        
        print("  computing downstream gradient")
        processNodes()
        
        print("  saving results")
        writeResults(conn)
        
        print("  computing accessiblity per species")
        computeAccessibility(conn)
        
    print("done")

    
if __name__ == "__main__":
    main() 
