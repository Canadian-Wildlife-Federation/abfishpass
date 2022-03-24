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
# this script computes mainstems, and route measures. Mainstems are based on
# longest upstream length and stream name.
#
# Assumptions:
#  * data is in an equal area projection such that st_length returns the length of a geometry in meters
#  * stream network forms a tree structure. There is only one output edge from each node. 
#
# Requirements: 
#  * stream_name field associated with the geometries
#  * elevation processing is completed
#
import appconfig
import shapely.wkb
from collections import deque
import uuid;
import psycopg2.extras


dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
watershed_id = appconfig.config['PROCESSING']['watershed_id']

dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
dbMainstemField = appconfig.config['MAINSTEM_PROCESSING']['mainstem_id']
dbDownMeasureField = appconfig.config['MAINSTEM_PROCESSING']['downstream_route_measure']
dbUpMeasureField = appconfig.config['MAINSTEM_PROCESSING']['upstream_route_measure']
edges = []
nodes = dict()

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        self.uplength = 0
        self.upedge = None
        self.mainstemid = None
        self.downstreammeasure = 0
   
    def addInEdge(self, edge):
        self.inedges.append(edge)
   
    def addOutEdge(self, edge):
        self.outedges.append(edge)
    
   
    
class Edge:
    def __init__(self, fromnode, tonode, fid, length, sname, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.fid = fid
        self.visited = False
        self.length = length
        self.sname = sname
        self.mainstemid = None
        self.uplength = 0
        self.downstreammeasure = 0
        
def createNetwork(connection):
    query = f"""
        SELECT a.{appconfig.dbIdField} as id, st_length(a.{appconfig.dbGeomField}) as length, 
          a.stream_name, a.{appconfig.dbGeomField}
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            fid = feature[0]
            length = feature[1]
            sname = feature[2]
            if (sname == "UNNAMED"):
                sname = None
                
            geom = shapely.wkb.loads(feature[3] , hex=True)
            
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
            
            edge = Edge(fromNode, toNode, fid, length, sname, geom)
            edges.append(edge)
            
            fromNode.addOutEdge(edge)
            toNode.addInEdge(edge)            

def processNodes():
    
    
    #walk down network        
    toprocess = deque()
    for edge in edges:
        edge.visited = False
        
    for node in nodes.values():
        if (len(node.inedges) == 0):
            toprocess.append(node)
            
    while (toprocess):
        node = toprocess.popleft()
        
        allvisited = True
        
        maxValue = 0 
        for inedge in node.inedges:
            if not inedge.visited:
                allvisited = False;
                break;
            else:
                length = inedge.fromNode.uplength + inedge.length
                if (length > maxValue):
                    maxValue = length
        
        if not allvisited:
            toprocess.append(node)
        else:
            node.uplength = maxValue
            
        for outedge in node.outedges:
            outedge.visited = True
            if (not outedge.toNode in toprocess):
                toprocess.append(outedge.toNode)
    
    #walk up computing mainstem id
    for edge in edges:
        edge.visited = False
        
    toprocess = deque()
    for node in nodes.values():
        if (len(node.outedges) == 0):
            toprocess.append(node)
            node.mainstemid = uuid.uuid4()
    
    while (toprocess):
        node = toprocess.popleft()
        
        if (len(node.inedges) == 0):
            continue
        
        #visit this node
        sname = None
        if (len(node.outedges) > 0):
            sname = node.outedges[0].sname
        
        longest = -9999
        longestNode = None
        
        namedNode = None
        
        longestnamed = -9999
        longestnamedNode = None
        
        for inedge in node.inedges:
            if (inedge.fromNode.uplength + inedge.length > longest):
                longest = inedge.fromNode.uplength + inedge.length
                longestNode = inedge.fromNode
            
            if (sname != None and inedge.sname == sname):
                namedNode = inedge.fromNode
            
            if (inedge.sname != None and (sname != None and inedge.sname != sname)):
                if (inedge.fromNode.uplength > longestnamed):
                    longestnamed = inedge.fromNode.uplength;
                    longestnamedNode = inedge.fromNode;
                    
        
        
        upnode = None
        if (namedNode != None):
            upnode = namedNode
        elif (longestnamedNode != None):
            upnode = longestnamedNode
        elif (longestNode != None):
            upnode = longestNode 
        
        for inedge in node.inedges:
            if (inedge.fromNode == upnode):
                inedge.mainstemid = node.mainstemid
                inedge.fromNode.downstreammeasure = node.downstreammeasure + inedge.length;
                inedge.downstreammeasure = node.downstreammeasure;
            else:
                inedge.mainstemid = uuid.uuid4()
                inedge.downstreammeasure = 0;
                inedge.fromNode.downstreammeasure = inedge.length

            inedge.fromNode.mainstemid = inedge.mainstemid
            
            toprocess.append(inedge.fromNode)
    
        
def writeResults(connection):
      
    updatequery = f"""
        UPDATE {dbTargetSchema}.{dbTargetStreamTable}
        SET {dbMainstemField} = %s, {dbDownMeasureField} = %s, {dbUpMeasureField} = %s
        WHERE {appconfig.dbIdField} = %s;
    """
    
    newdata = []
    
    for edge in edges:
        newdata.append( (edge.mainstemid, edge.downstreammeasure, edge.downstreammeasure + edge.length, edge.fid) )
    
    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata);
            
    connection.commit()


#--- main program ---    
with appconfig.connectdb() as conn:
    
    conn.autocommit = False
    
    print("Computing Mainstems")
    print("  creating output column")
    #add a new geometry column for output removing existing one
    query = f"""
        alter table {dbTargetSchema}.{dbTargetStreamTable} 
            add column if not exists {dbMainstemField} uuid;
            
        alter table {dbTargetSchema}.{dbTargetStreamTable} 
            add column if not exists {dbDownMeasureField} double precision;
        
        alter table {dbTargetSchema}.{dbTargetStreamTable} 
            add column if not exists {dbUpMeasureField} double precision;
        
        update {dbTargetSchema}.{dbTargetStreamTable} 
          set {dbMainstemField} = null, 
          {dbDownMeasureField} = null, 
          {dbUpMeasureField} = null;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    
    print("  creating network")
    createNetwork(conn)
    
    print("  processing nodes")
    processNodes()
        
    print("  writing results")
    writeResults(conn)
    
print("done")