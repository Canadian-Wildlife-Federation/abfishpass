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
# This script computes barrier attributes that
# require stream network traversal (upstream length per habitat type)
#

import appconfig
import shapely.wkb
from collections import deque
import psycopg2.extras

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']

edges = []
nodes = dict()
species = []

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        self.barrierids = set()
   
    def addInEdge(self, edge):
        self.inedges.append(edge)
   
    def addOutEdge(self, edge):
        self.outedges.append(edge)
    
class Edge:
    def __init__(self, fromnode, tonode, fid, length, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.length = length
        self.fid = fid
        self.visited = False
        self.speca = {} # species accessibility
        self.specaup = {} # species accessibility upstream
        self.habitat = {}
        self.habitatup = {}
        self.funchabitatup = {}
        self.upbarriercnt = 0
    
    def print(self):
        print("fid:", self.fid)
        print("habitat:", self.habitat)
        print("habitatup:", self.habitatup)

    # def check_spawn_habitat_all(self):
    #     result = any(val == True for val in self.spawn_habitat.values())
    #     return result
    
    # def check_rear_habitat_all(self):
    #     result = any(val == True for val in self.rear_habitat.values())
    #     return result

    # def check_habitat_all(self):
    #     result = any(val == True for val in self.habitat.values())
    #     return result

def createNetwork(connection):
    
    
    query = f"""
        SELECT a.code
        FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable} a
    """
    

    accessibilitymodel = ''
    habitatmodel = ''
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        for feature in features:
            species.append(feature[0])
            accessibilitymodel = accessibilitymodel + ', ' + feature[0] + '_accessibility'
            habitatmodel = habitatmodel + ', habitat_' + feature[0]
    
    
    query = f"""
        SELECT a.{appconfig.dbIdField} as id, 
            st_length(a.{appconfig.dbGeomField}), a.{appconfig.dbGeomField},
            barrier_up_cnt
            {accessibilitymodel} {habitatmodel}
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        for feature in features:
            fid = feature[0]
            length = feature[1]
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
            
            edge = Edge(fromNode, toNode, fid, length, geom)
            edge.upbarriercnt = feature[3]
            index = 4
            for fish in species:
                edge.speca[fish] = feature[index]
                edge.habitat[fish] = feature[index + len(species)]
                index = index + 1

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
        
        uplength = {}
        habitat = {}
        funchabitat = {}
        
        for fish in species:
            uplength[fish] = 0
            habitat[fish] = 0
            funchabitat[fish] = 0
        
        outbarriercnt = 0
        
        for inedge in node.inedges:
            outbarriercnt += inedge.upbarriercnt
                
            if not inedge.visited:
                allvisited = False
                break
            else:
                for fish in species:
                    uplength[fish] = uplength[fish] + inedge.specaup[fish]
                    habitat[fish] = habitat[fish] + inedge.habitatup[fish]
                    funchabitat[fish] = funchabitat[fish] + inedge.funchabitatup[fish]
                
        if not allvisited:
            toprocess.append(node)
        else:
        
            for outedge in node.outedges:

                for fish in species:

                    if (outedge.speca[fish] == appconfig.Accessibility.ACCESSIBLE.value or outedge.speca[fish] == appconfig.Accessibility.POTENTIAL.value):
                        outedge.specaup[fish] = uplength[fish] + outedge.length
                    else:
                        outedge.specaup[fish] = uplength[fish]

                    if outedge.habitat[fish]:
                        outedge.habitatup[fish] = habitat[fish] + outedge.length
                    else:
                        outedge.habitatup[fish] = habitat[fish]

                    if outedge.upbarriercnt != outbarriercnt:
                        if outedge.habitat[fish]:
                            outedge.funchabitatup[fish] = outedge.length
                        else:
                            outedge.funchabitatup[fish] = 0
                    elif outedge.habitat[fish]:
                        outedge.funchabitatup[fish] = funchabitat[fish] + outedge.length
                    else: 
                        outedge.funchabitatup[fish] = funchabitat[fish]

                outedge.visited = True
                if (not outedge.toNode in toprocess):
                    toprocess.append(outedge.toNode)
        
def writeResults(connection):
      
    tablestr = ''
    inserttablestr = ''
    for fish in species:
        tablestr = tablestr + ', total_upstr_pot_access_' + fish + ' double precision'
        tablestr = tablestr + ', total_upstr_hab_' + fish + ' double precision'
        tablestr = tablestr + ', func_upstr_hab_' + fish + ' double precision'
        inserttablestr = inserttablestr + ",%s,%s,%s"

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.temp;
        
        CREATE TABLE {dbTargetSchema}.temp (
            stream_id uuid
            {tablestr}
        );
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
    
    
    updatequery = f"""    
        INSERT INTO {dbTargetSchema}.temp VALUES (%s {inserttablestr}) 
    """

    newdata = []
    
    for edge in edges:
        
        data = []
        data.append(edge.fid)
        for fish in species:
            data.append (edge.specaup[fish])
            data.append (edge.habitatup[fish])
            data.append (edge.funchabitatup[fish])

        newdata.append( data )

    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata)
            
    for fish in species:
        
        query = f"""
            --upstream potentially accessible
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_pot_access_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_pot_access_{fish} double precision;
            
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_pot_access_{fish} = a.total_upstr_pot_access_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a, {dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND
                  a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;


            --total upstream habitat
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_{fish} double precision;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_hab_{fish} = a.total_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            
            --functional upstream habitat
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_{fish} double precision;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET func_upstr_hab_{fish} = a.func_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        """
        with connection.cursor() as cursor:
            cursor.execute(query)
    
    query = f"""
        DROP TABLE {dbTargetSchema}.temp;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)        

    connection.commit()


def assignBarrierCounts(connection):
    
    query = f"""
        UPDATE {dbTargetSchema}.{dbBarrierTable}
        SET 
            barrier_cnt_upstr = a.barrier_up_cnt,
            barriers_upstr = a.barriers_up,
            gradient_barrier_cnt_upstr = a.gradient_barrier_up_cnt
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id =  {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
        
        UPDATE {dbTargetSchema}.{dbBarrierTable}
        SET
            barrier_cnt_downstr = a.barrier_down_cnt,
            barriers_downstr = a.barriers_down,
            gradient_barrier_cnt_downstr = a.gradient_barrier_down_cnt
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id =  {dbTargetSchema}.{dbBarrierTable}.stream_id_down;
        
    """
    with connection.cursor() as cursor:
        cursor.execute(query)             

    connection.commit()
    
    
#--- main program ---
def main():

    edges.clear()
    nodes.clear()
    species.clear()    
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Habitat Models for Barriers")
        
        print("  assigning barrier counts")
        assignBarrierCounts(conn)
        
        print("  creating network")
        createNetwork(conn)
        
        print("  processing nodes")
        processNodes()
            
        print("  writing results")
        writeResults(conn)
        
    print("done")
    

if __name__ == "__main__":
    main()      
