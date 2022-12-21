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
        self.spawn_habitat = {}
        self.spawn_habitatup = {}
        self.rear_habitat = {}
        self.rear_habitatup = {}
        self.habitat = {}
        self.habitatup = {}
        self.spawn_funchabitatup = {}
        self.rear_funchabitatup = {}
        self.funchabitatup = {}
        self.spawn_habitat_all = None
        self.spawn_habitatup_all = {}
        self.rear_habitat_all = None
        self.rear_habitatup_all = {}
        self.habitat_all = None
        self.habitatup_all = {}
        self.spawn_funchabitat_all = {}
        self.rear_funchabitat_all = {}
        self.funchabitat_all = {}
        self.spawn_funchabitatup_all = {}
        self.rear_funchabitatup_all = {}
        self.funchabitatup_all = {}

        self.upbarriercnt = 0
    
    def print(self):
        print("fid:", self.fid)
        print("spawn_habitat:", self.spawn_habitat)
        print("rear_habitat:", self.rear_habitat)
        print("habitat:", self.habitat)
        print("spawn_habitatup:", self.spawn_habitatup)
        print("rear_habitatup:", self.rear_habitatup)
        print("habitatup:", self.habitatup)
        print("spawn_habitat_all:", self.spawn_habitat_all)
        print("rear_habitat_all:", self.rear_habitat_all)
        print("habitat_all:", self.habitat_all)

    def check_spawn_habitat_all(self):
        result = any(val == True for val in self.spawn_habitat.values())
        return result
    
    def check_rear_habitat_all(self):
        result = any(val == True for val in self.rear_habitat.values())
        return result

    def check_habitat_all(self):
        result = any(val == True for val in self.habitat.values())
        return result

def createNetwork(connection):
    
    
    query = f"""
        SELECT a.code
        FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable} a
    """
    

    accessibilitymodel = ''
    spawnhabitatmodel = ''
    rearhabitatmodel = ''
    habitatmodel = ''
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        for feature in features:
            species.append(feature[0])
            accessibilitymodel = accessibilitymodel + ', ' + feature[0] + '_accessibility'
            spawnhabitatmodel = spawnhabitatmodel + ', habitat_spawn_' + feature[0]
            rearhabitatmodel = rearhabitatmodel + ', habitat_rear_' + feature[0]
            habitatmodel = habitatmodel + ', habitat_' + feature[0]
    
    
    query = f"""
        SELECT a.{appconfig.dbIdField} as id, 
            st_length(a.{appconfig.dbGeomField}), a.{appconfig.dbGeomField},
            barrier_up_cnt
            {accessibilitymodel} {spawnhabitatmodel} {rearhabitatmodel} {habitatmodel}
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
                edge.spawn_habitat[fish] = feature[index + len(species)]
                edge.rear_habitat[fish] = feature[index + len(species)]
                edge.habitat[fish] = feature[index + len(species)]
                index = index + 1

            edge.spawn_habitat_all = edge.check_spawn_habitat_all()
            edge.rear_habitat_all = edge.check_rear_habitat_all()
            edge.habitat_all = edge.check_habitat_all()

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
        spawn_habitat = {}
        rear_habitat = {}
        habitat = {}
        spawn_funchabitat = {}
        rear_funchabitat = {}
        funchabitat = {}
        spawn_habitat_all = 0
        rear_habitat_all = 0
        habitat_all = 0
        spawn_funchabitat_all = 0
        rear_funchabitat_all = 0
        funchabitat_all = 0
        
        for fish in species:
            uplength[fish] = 0
            spawn_habitat[fish] = 0
            rear_habitat[fish] = 0
            habitat[fish] = 0
            spawn_funchabitat[fish] = 0
            rear_funchabitat[fish] = 0
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
                    spawn_habitat[fish] = spawn_habitat[fish] + inedge.spawn_habitatup[fish]
                    rear_habitat[fish] = rear_habitat[fish] + inedge.rear_habitatup[fish]
                    habitat[fish] = habitat[fish] + inedge.habitatup[fish]
                    spawn_funchabitat[fish] = spawn_funchabitat[fish] + inedge.spawn_funchabitatup[fish]
                    rear_funchabitat[fish] = rear_funchabitat[fish] + inedge.rear_funchabitatup[fish]
                    funchabitat[fish] = funchabitat[fish] + inedge.funchabitatup[fish]
                
                spawn_habitat_all = spawn_habitat_all + inedge.spawn_habitatup_all
                rear_habitat_all = rear_habitat_all + inedge.rear_habitatup_all
                habitat_all = habitat_all + inedge.habitatup_all
                
        if not allvisited:
            toprocess.append(node)
        else:
        
            for outedge in node.outedges:
                
                for fish in species:
                    if (outedge.speca[fish] == appconfig.Accessibility.ACCESSIBLE.value or outedge.speca[fish] == appconfig.Accessibility.POTENTIAL.value):
                        outedge.specaup[fish] = uplength[fish] + outedge.length
                    else:
                        outedge.specaup[fish] = uplength[fish]
                        
                    if outedge.spawn_habitat[fish]:
                        outedge.spawn_habitatup[fish] = spawn_habitat[fish] + outedge.length
                    else:
                        outedge.spawn_habitatup[fish] = spawn_habitat[fish]
                    
                    if outedge.rear_habitat[fish]:
                        outedge.rear_habitatup[fish] = rear_habitat[fish] + outedge.length
                    else:
                        outedge.rear_habitatup[fish] = rear_habitat[fish]
                    
                    if outedge.habitat[fish]:
                        outedge.habitatup[fish] = habitat[fish] + outedge.length
                    else:
                        outedge.habitatup[fish] = habitat[fish]
                        
                    if outedge.upbarriercnt != outbarriercnt:
                        outedge.spawn_funchabitatup[fish] = outedge.length
                    elif outedge.spawn_habitat[fish]:
                        outedge.spawn_funchabitatup[fish] = spawn_funchabitat[fish] + outedge.length
                    else: 
                        outedge.spawn_funchabitatup[fish] = spawn_funchabitat[fish]

                    if outedge.upbarriercnt != outbarriercnt:
                        outedge.rear_funchabitatup[fish] = outedge.length
                    elif outedge.rear_habitat[fish]:
                        outedge.rear_funchabitatup[fish] = rear_funchabitat[fish] + outedge.length
                    else: 
                        outedge.rear_funchabitatup[fish] = rear_funchabitat[fish]

                    if outedge.upbarriercnt != outbarriercnt:
                        outedge.funchabitatup[fish] = outedge.length
                    elif outedge.habitat[fish]:
                        outedge.funchabitatup[fish] = funchabitat[fish] + outedge.length
                    else: 
                        outedge.funchabitatup[fish] = funchabitat[fish]
                
                if outedge.spawn_habitat_all:
                    outedge.spawn_habitatup_all = spawn_habitat_all + outedge.length
                else:
                    outedge.spawn_habitatup_all = spawn_habitat_all

                if outedge.rear_habitat_all:
                    outedge.rear_habitatup_all = rear_habitat_all + outedge.length
                else:
                    outedge.rear_habitatup_all = rear_habitat_all

                if outedge.habitat_all:
                    outedge.habitatup_all = habitat_all + outedge.length
                else:
                    outedge.habitatup_all = habitat_all
                
                if outedge.upbarriercnt != outbarriercnt:
                    outedge.spawn_funchabitatup_all = outedge.length
                elif outedge.spawn_habitat_all:
                    outedge.spawn_funchabitatup_all = spawn_funchabitat_all + outedge.length
                else: 
                    outedge.spawn_funchabitatup_all = spawn_funchabitat_all

                if outedge.upbarriercnt != outbarriercnt:
                    outedge.rear_funchabitatup_all = outedge.length
                elif outedge.rear_habitat_all:
                    outedge.rear_funchabitatup_all = rear_funchabitat_all + outedge.length
                else: 
                    outedge.rear_funchabitatup_all = rear_funchabitat_all

                if outedge.upbarriercnt != outbarriercnt:
                    outedge.funchabitatup_all = outedge.length
                elif outedge.habitat_all:
                    outedge.funchabitatup_all = funchabitat_all + outedge.length
                else: 
                    outedge.funchabitatup_all = funchabitat_all

                outedge.visited = True
                if (not outedge.toNode in toprocess):
                    toprocess.append(outedge.toNode)
        
def writeResults(connection):
      
    tablestr = ''
    inserttablestr = ''
    for fish in species:
        tablestr = tablestr + ', total_upstr_pot_access_' + fish + ' numeric'
        tablestr = tablestr + ', total_upstr_hab_spawn_' + fish + ' numeric'
        tablestr = tablestr + ', total_upstr_hab_rear_' + fish + ' numeric'
        tablestr = tablestr + ', total_upstr_hab_' + fish + ' numeric'
        tablestr = tablestr + ', func_upstr_hab_spawn_' + fish + ' numeric'
        tablestr = tablestr + ', func_upstr_hab_rear_' + fish + ' numeric'
        tablestr = tablestr + ', func_upstr_hab_' + fish + ' numeric'
        inserttablestr = inserttablestr + ",%s,%s,%s,%s,%s,%s,%s"

    tablestr = tablestr + ', total_upstr_hab_spawn_all' + ' numeric'
    tablestr = tablestr + ', total_upstr_hab_rear_all' + ' numeric'
    tablestr = tablestr + ', total_upstr_hab_all' + ' numeric'
    tablestr = tablestr + ', func_upstr_hab_spawn_all' + ' numeric'
    tablestr = tablestr + ', func_upstr_hab_rear_all' + ' numeric'
    tablestr = tablestr + ', func_upstr_hab_all' + ' numeric'
    inserttablestr = inserttablestr + ",%s,%s,%s,%s,%s,%s"

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
            data.append (edge.spawn_habitatup[fish])
            data.append (edge.rear_habitatup[fish])
            data.append (edge.habitatup[fish])
            data.append (edge.spawn_funchabitatup[fish])
            data.append (edge.rear_funchabitatup[fish])
            data.append (edge.funchabitatup[fish])
        
        data.append(edge.spawn_habitatup_all)
        data.append(edge.rear_habitatup_all)
        data.append(edge.habitatup_all)
        data.append(edge.spawn_funchabitatup_all)
        data.append(edge.rear_funchabitatup_all)
        data.append(edge.funchabitatup_all)

        newdata.append( data )

    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata)
            
    for fish in species:
        
        query = f"""
            --upstream potentially accessible
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_pot_access_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_pot_access_{fish} numeric;
            
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_pot_access_{fish} = a.total_upstr_pot_access_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a, {dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND
                  a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;


            --total upstream habitat
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_spawn_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_spawn_{fish} numeric;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_hab_spawn_{fish} = a.total_upstr_hab_spawn_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_rear_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_rear_{fish} numeric;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_hab_rear_{fish} = a.total_upstr_hab_rear_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_{fish} numeric;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET total_upstr_hab_{fish} = a.total_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            
            --functional upstream habitat
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_spawn_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_spawn_{fish} numeric;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET func_upstr_hab_spawn_{fish} = a.func_upstr_hab_spawn_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
            
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_rear_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_rear_{fish} numeric;
    
            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET func_upstr_hab_rear_{fish} = a.func_upstr_hab_rear_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_{fish} numeric;

            UPDATE {dbTargetSchema}.{dbBarrierTable} 
            SET func_upstr_hab_{fish} = a.func_upstr_hab_{fish} / 1000.0 
            FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
            WHERE a.stream_id = b.id AND 
                a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        """
        with connection.cursor() as cursor:
            cursor.execute(query)
    
    query = f"""
        --total upstream habitat - all
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_spawn_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_spawn_all numeric;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET total_upstr_hab_spawn_all = a.total_upstr_hab_spawn_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_rear_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_rear_all numeric;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET total_upstr_hab_rear_all = a.total_upstr_hab_rear_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS total_upstr_hab_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN total_upstr_hab_all numeric;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET total_upstr_hab_all = a.total_upstr_hab_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;


        --functional upstream habitat - all
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_spawn_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_spawn_all numeric;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET func_upstr_hab_spawn_all = a.func_upstr_hab_spawn_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;

        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_rear_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_rear_all numeric;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET func_upstr_hab_rear_all = a.func_upstr_hab_rear_all / 1000.0 
        FROM {dbTargetSchema}.temp a,{dbTargetSchema}.{dbTargetStreamTable} b 
        WHERE a.stream_id = b.id AND 
            a.stream_id = {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
        
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS func_upstr_hab_all;
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN func_upstr_hab_all numeric;

        UPDATE {dbTargetSchema}.{dbBarrierTable} 
        SET func_upstr_hab_all = a.func_upstr_hab_all / 1000.0 
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


def assignBarrierSpeciesCounts(connection):
    
    query = f"""
        UPDATE {dbTargetSchema}.{dbBarrierTable}
        SET species_upstr = a.fish_survey_up,
            stock_upstr = a.fish_stock_up,
            barrier_cnt_upstr = a.barrier_up_cnt,
            barriers_upstr = a.barriers_up,
            gradient_barrier_cnt_upstr = a.gradient_barrier_up_cnt
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
        WHERE a.id =  {dbTargetSchema}.{dbBarrierTable}.stream_id_up;
        
        UPDATE {dbTargetSchema}.{dbBarrierTable}
        SET species_downstr = a.fish_survey_down,
            stock_downstr = a.fish_stock_down,
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
        
        print("  assigning barrier and species counts")
        assignBarrierSpeciesCounts(conn)
        
        print("  creating network")
        createNetwork(conn)
        
        print("  processing nodes")
        processNodes()
            
        print("  writing results")
        writeResults(conn)
        
    print("done")
    

if __name__ == "__main__":
    main()      
