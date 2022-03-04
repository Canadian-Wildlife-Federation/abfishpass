'''
Script to load gdb files into postgis
database

'''
import appconfig
import shapely.wkb
import shapely.geometry
import psycopg2.extras
from collections import deque

dbTargetSchema = appconfig.config['PROCESSING']['output_schema']

dbTargetTable = appconfig.config['ELEVATION_PROCESSING']['target_table']
dbSourceGeom = appconfig.config['ELEVATION_PROCESSING']['target_3dgeometry_field']
dbTargetGeom = appconfig.config['ELEVATION_PROCESSING']['target_smoothedgeometry_field']
    
edges = []
nodes = dict()

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        self.z = appconfig.NODATA
        self.minvalue = self.z
        self.maxvalue = self.z
        

    def addInEdge(self, edge):
        self.inedges.append(edge)
        self.addZ(edge.ls.coords[len(edge.ls.coords) - 1][2])

    def addOutEdge(self, edge):
        self.outedges.append(edge)
        self.addZ(edge.ls.coords[0][2])
    
    def addZ(self, z):
        if (self.z == appconfig.NODATA or self.z == z):
            self.z = z
        else:
            print("DIFFERENT Z VALUES AT SAME POSITION")
    
class Edge:
    def __init__(self, fromnode, tonode, fid, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.fid = fid
        self.visited = False
        self.newz = [appconfig.NODATA for i in range(len(ls.coords))]
        
def createNetwork(connection):
    query = f"""
        SELECT id, {dbSourceGeom}
        FROM {dbTargetSchema}.{dbTargetTable}
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            geom = shapely.wkb.loads(feature[1] , hex=True)
            fid = feature[0]
            
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
            
            edge = Edge(fromNode, toNode, fid, geom)
            edges.append(edge)
            
            fromNode.addOutEdge(edge)
            toNode.addInEdge(edge)            

def processNodes():
    
    #walk up network
    for edge in edges:
        edge.visited = False
        
    toprocess = deque()
    for node in nodes.values():
        if (len(node.outedges) == 0):
            toprocess.append(node)
            node.maxvalue = node.z
    
    while (toprocess):
        node = toprocess.popleft()
        
        allvisited = True
        for outedge in node.outedges:
            if not outedge.visited:
                allvisited = False;
                break;
        if not allvisited:
            toprocess.append(node)
        else:
            #visit this node
            for inedge in node.inedges:
                inedge.fromNode.maxvalue = max(node.maxvalue, inedge.fromNode.z)
                inedge.visited = True
                toprocess.append(inedge.fromNode)
    
    #walk down network        
    toprocess = deque()
    for edge in edges:
        edge.visited = False
        
    for node in nodes.values():
        if (len(node.inedges) == 0):
            node.minvalue = node.z
            toprocess.append(node)
            
    while (toprocess):
        node = toprocess.popleft()
        
        allvisited = True
        for inedge in node.inedges:
            if not inedge.visited:
                allvisited = False;
                break;
        if not allvisited:
            toprocess.append(node)
        else:
            #visit this node
            for outedge in node.outedges:
                if (node.minvalue == appconfig.NODATA and outedge.toNode.z == appconfig.NODATA):
                    #no data
                    outedge.toNode.minvalue = outedge.toNode.z
                elif (node.minvalue == appconfig.NODATA):
                    outedge.toNode.minvalue = outedge.toNode.z
                elif (outedge.toNode.z == appconfig.NODATA):
                    outedge.toNode.minvalue = node.minvalue
                else:        
                    outedge.toNode.minvalue = min(node.minvalue, outedge.toNode.z)
                
                outedge.visited = True  
                toprocess.append(outedge.toNode)     
    
    #update z values 
    for node in nodes.values():
        node.z = (node.maxvalue + node.minvalue) / 2.0
        
    for edge in edges:
        edge.newz[0] = edge.fromNode.z
        edge.newz[len(edge.ls.coords)-1] = edge.toNode.z


def processEdges():   
    
    for edge in edges:             
        ls = edge.ls
        
        size = len(ls.coords)
        
        minvalues = [appconfig.NODATA] * size
        maxvalues = [appconfig.NODATA] * size
        
        absmax = edge.newz[0]
        absmin = edge.newz[size - 1]
        
        minvalues [0] = edge.newz[0]
        maxvalues [size - 1] = edge.newz[size - 1]
        
        
        for i in range(1, size):
            temp = ls.coords[i][2]
            if (temp < absmin):
                temp = absmin
            minv = min(temp, minvalues[i-1])
            minvalues[i] = minv
            
            temp = ls.coords[size - 1 - i][2]
            if (temp > absmax):
                temp = absmax
            maxv = max(temp, maxvalues[size - i])
            maxvalues [size - 1 - i ] = maxv
        
        for i in range(0, size):
            edge.newz[i] = ((minvalues[i] + maxvalues[i]) / 2.0)
        
        
def writeResults(connection):
    
    updatequery = f"""
        UPDATE {dbTargetSchema}.{dbTargetTable} 
        set {dbTargetGeom} = st_setsrid(st_geomfromwkb(%s),{appconfig.dataSrid})
        WHERE id = %s
    """
    
    newdata = []
    
    for edge in edges:
        newpnts = [];
        for i in range(0, len(edge.ls.coords)):
            x = edge.ls.coords[i][0]
            y = edge.ls.coords[i][1]
            z = edge.newz[i]
            newpnts.append((x,y,z))
        ls = shapely.geometry.LineString(newpnts)
        newdata.append( (shapely.wkb.dumps(ls), edge.fid))
    
    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata);
            
    connection.commit()
    

#--- main program ---    
with appconfig.connectdb() as conn:
    
    conn.autocommit = False
    
    print("Smoothing Elevation Values")
    print("  creating output column")
    #add a new geometry column for output removing existing one
    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} drop column if exists {dbTargetGeom};        
        ALTER TABLE {dbTargetSchema}.{dbTargetTable} add column {dbTargetGeom} geometry(linestringz, {appconfig.dataSrid});
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    
    print("  creating network")
    createNetwork(conn)
    
    print("  processing nodes")
    processNodes()
    
    print("  processing edges")
    processEdges()
    
    print("  writing results")
    writeResults(conn)
    
print("done")
      
#drop table working.points3d; 
#
#create table working.points3d as 
#select geom, st_z(geom) as z
#from (
#  SELECT (ST_DumpPoints(geometry_raw3d)).geom  AS geom
#  FROM working.stream3d
#) foo
    