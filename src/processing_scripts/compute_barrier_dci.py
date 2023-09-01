import appconfig
import psycopg2.extras
import numpy as np
import uuid

iniSection = appconfig.args.args[0]
dbTargetSchema = appconfig.config[iniSection]['output_schema']
watershed_id = appconfig.config[iniSection]['watershed_id']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']

class StreamData:
    def __init__(self, fid, length, downbarriers, habitat):
        self.fid = fid
        self.length = length
        self.downbarriers = downbarriers
        self.habitat = habitat
        self.downpassability = {}
        self.dci = {}
    
    def print(self):
        print("fid:", self.fid)
        print("downbarriers:", self.downbarriers)
        print("downpassability:", self.downpassability)
        print("habitat:", self.habitat)

class BarrierData:
    def __init__(self, bid, passabilitystatus):
        self.bid = bid
        self.passabilitystatus = passabilitystatus
        self.dci = {}
    
    def print(self):
        print("bid:", self.bid)
        print("passability status:", self.passabilitystatus)
        print("dci:", self.dci)

def getSpeciesConnectivity(conn, species):

    dci_base = {}

    for fish in species:

        query = f"""
            SELECT SUM(dci_{fish}) FROM {dbTargetSchema}.{dbTargetStreamTable};
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            d = cursor.fetchone()
            dcib = float(d[0])
            dci_base[fish] = dcib

    return dci_base

def getBarrierDCI(barrier, barrierData, streamData, species, speciesDCI, totalHabitat):

    newStreamArray = []
    dci_sum = {}

    for stream in streamData:
        streamDCI = {}
        downbarriers = stream.downbarriers
        downpassability = stream.downpassability
        newStreamData = StreamData(stream.fid, stream.length, downbarriers, stream.habitat)

        for fish in species:
            downbarriers[fish] = stream.downbarriers[fish]
            passabilities = []
            
            for b in downbarriers[fish]:
                if str(b) == str(barrier.bid):
                    pass
                else:
                    passabilities.append(barrierData[uuid.UUID(b)].passabilitystatus[fish])
            
            downpassability[fish] = np.prod(passabilities)
            newStreamData.downpassability[fish] = downpassability[fish]

            if newStreamData.habitat[fish]:
                streamDCI[fish] = ((newStreamData.length / totalHabitat[fish]) * newStreamData.downpassability[fish]) * 100
            else:
                streamDCI[fish] = 0
            
            newStreamData.dci[fish] = streamDCI[fish]
            
        newStreamArray.append(newStreamData)
    
    for fish in species:
        dci_sum[fish] = sum(newStream.dci[fish] for newStream in newStreamArray)
        barrier.dci[fish] = round((dci_sum[fish] - speciesDCI[fish]),4)

    return barrier.dci

def getHabitatLength(conn, species):

    totalLength = {}

    for fish in species:
        query = f"""
            SELECT SUM(segment_length) FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE habitat_{fish} = true;
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            length = cursor.fetchone()
            totalLength[fish] = length[0]
    
    return totalLength

def generateStreamData(conn, species):

    streamArray = []

    barrierdownmodel = ''
    habitatmodel = ''

    for fish in species:
        barrierdownmodel = barrierdownmodel + ', barriers_down_' + fish
        habitatmodel = habitatmodel + ', habitat_' + fish

    query = f"""
    SELECT a.{appconfig.dbIdField} as id,
        segment_length
        {barrierdownmodel}
        {habitatmodel}
    FROM {dbTargetSchema}.{dbTargetStreamTable} a
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        allstreamdata = cursor.fetchall()
        
        for stream in allstreamdata:

            fid = stream[0]
            length = stream[1]
            downbarriers = {}
            habitat = {}

            index = 2

            for fish in species:
                downbarriers[fish] = stream[index]
                habitat[fish] = stream[index + len(species)]
                index = index + 1
            
            streamArray.append(StreamData(fid, length, downbarriers, habitat))

    return streamArray

def generateBarrierData(conn, species):

    barrierDict = {}

    passabilitymodel = ''

    for fish in species:
        passabilitymodel = passabilitymodel + ', passability_status_' + fish

    query = f"""
    SELECT id {passabilitymodel} FROM {dbTargetSchema}.{dbBarrierTable};
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        allbarrierdata = cursor.fetchall()
        
        for barrier in allbarrierdata:

            bid = barrier[0]
            passabilitystatus = {}

            index = 1

            for fish in species:
                passabilitystatus[fish] = float(0 if barrier[index] is None else barrier[index])
                index = index + 1
            
            barrierDict[bid] = BarrierData(bid, passabilitystatus)

    return barrierDict

def writeResults(conn, newAllBarrierData, species):
    
    tablestr = ''
    inserttablestr = ''

    for fish in species:
        tablestr = tablestr + ', dci_' + fish + ' double precision'
        inserttablestr = inserttablestr + ",%s"

    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}.temp;
        
        CREATE TABLE {dbTargetSchema}.temp (
            barrier_id uuid
            {tablestr}
        );
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    
    updatequery = f"""    
        INSERT INTO {dbTargetSchema}.temp VALUES (%s {inserttablestr}) 
    """

    newdata = []
    
    for record in newAllBarrierData:
        data = []
        data.append(record.bid)
        for fish in species:
            data.append(record.dci[fish])

        newdata.append(data)

    with conn.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata)

    for fish in species:
        
        query = f"""
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} DROP COLUMN IF EXISTS dci_{fish};
            ALTER TABLE {dbTargetSchema}.{dbBarrierTable} ADD COLUMN dci_{fish} double precision;
            
            UPDATE {dbTargetSchema}.{dbBarrierTable}
            SET dci_{fish} = a.dci_{fish}
            FROM {dbTargetSchema}.temp a
            WHERE a.barrier_id = id;

        """
        with conn.cursor() as cursor:
            cursor.execute(query)

    conn.commit()

def main():

    print("Started!")
    with appconfig.connectdb() as conn:
        conn.autocommit = False

        species = []

        query = f"""
            SELECT a.code
            FROM {appconfig.dataSchema}.{appconfig.fishSpeciesTable} a
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            spec = cursor.fetchall()
            for s in spec:
                species.append(s[0])

        print("species list: ", species)
        
        speciesDCI = getSpeciesConnectivity(conn, species)

        totalHabitat = getHabitatLength(conn, species)

        streamData = generateStreamData(conn, species)

        barrierData = generateBarrierData(conn, species)

        newAllBarrierData = []

        for barrierid in barrierData:
            print("barrier id:", barrierid, "object:", barrierData[barrierid])
            dci = getBarrierDCI(barrierData[barrierid], barrierData, streamData, species, speciesDCI, totalHabitat)
            newBarrierData = BarrierData(barrierData[barrierid].bid, barrierData[barrierid].passabilitystatus)
            newBarrierData.dci = dci
            newAllBarrierData.append(newBarrierData)

        writeResults(conn, newAllBarrierData, species)

        print("Done!")

if __name__ == "__main__":
    main()      