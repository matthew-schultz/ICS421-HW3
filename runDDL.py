#runDDL.py
import configparser
import socket
import sqlite3
import sys
from threading import Thread
# import Error

def SendDDLToNode(ddlSQL, dbhost, dbport, nodeNum, catDbName, nodeDbName):
    print('runDDL.py: connecting to host ' + dbhost)

    mySocket = socket.socket()
    try:
        mySocket.connect((dbhost, dbport))
        packet = '<dbname>' + nodeDbName + '</dbname>' + ddlSQL
        print('runDDL.py: send data "' + packet + '"')
        mySocket.send(packet.encode())
        data = str(mySocket.recv(1024).decode())
        print('runDDL.py: recv ' + data + ' from host ' + dbhost)

        if(data == 'success'):
            tname = getTname(ddlSQL)
            # print('tname is ' + tname)
            catSQL = 'DELETE FROM dtables WHERE nodeid='+ str(nodeNum) + ';'            
            if SQLIsCreate(ddlSQL):
                # print ('ddlSQL is a create statement')
                # catSQL = 'TRUNCATE TABLE tablename;'
                catSQL = 'INSERT INTO dtables VALUES ("'+ tname +'","","' + dbhost + '","","",0,' + str(nodeNum) + ',NULL,NULL,NULL)'
            RunSQL(catSQL, catDbName)
            # print('runDDL.py: ' + catSQL)
            # print('')
    except OSError:
        print('runDDL.py: failed to connect to host ' + dbhost)
    mySocket.close()


def RunSQL(sql, dbname):
    print('runDDL.py: executing sql statement ' + sql) 
    try:
        sqlConn = sqlite3.connect(dbname)
        c = sqlConn.cursor()
        c.execute(sql)
        print(str(c.fetchall()))
        sqlConn.commit()
        sqlConn.close()
    except sqlite3.IntegrityError as e:
        print(e)
    except sqlite3.OperationalError as e:
        print(e)

def SQLIsCreate(sql):
    isInsert = False
    # remove leading whitespace from sql string and split
    sqlArray = sql.lstrip().split(" ")
    if sqlArray[0].upper() == 'CREATE':
        isInsert = True
    return isInsert


def CreateCatalog(dbname):
    sqlConn = sqlite3.connect(dbname)
    c = sqlConn.cursor()
    # catSQL = 'DROP TABLE dtables;\n'
    catSQL = '''CREATE TABLE IF not exists dtables(tname char(32), 
            nodedriver char(64), 
            nodeurl char(128), 
            nodeuser char(16), 
            nodepasswd char(16), 
            partmtd int, 
            nodeid int, 
            partcol char(32), 
            partparam1 char(32),
            partparam2 char(32),
            CONSTRAINT unique_nodeid UNIQUE(nodeid))'''
    # Create table
    tableCreatedMsg = ''
    try:
        c.execute(catSQL)
    except Error:
        tableCreatedMsg = 'failure'
    else:
        tableCreatedMsg = 'success'
    sqlConn.commit()
    sqlConn.close()
    return tableCreatedMsg


def getTname(data):
    tname = ''
    
    dataArray = data.split(' ')
    count = 0
    for d in dataArray:
        if d.upper() == 'TABLE':
            # check if table name will come after an 'if exists...' statement
            if dataArray[count + 1] == 'if':
                 tname = dataArray[count+4]
            else: tname = dataArray[count + 1]
        count = count + 1   
    tname = tname.split('(')[0] #remove trailing '('
    return tname


def Main():
    #get command line arguments
    if(len(sys.argv) >= 3):
        clustercfg = sys.argv[1]
        ddlfile = sys.argv[2]
        
        configDict = ParseConfig(clustercfg)
        # print('ParseConfig returned ' + str(configDict))

        #read ddlfile as a string to be executed as sql
        with open(ddlfile, 'r') as myfile:
            ddlSQL=myfile.read().replace('\n', '')
        # print("sql string is " + ddlSQL)
        catDbName = configDict['catalog.db']
        catMsg = CreateCatalog(catDbName)
        print(__file__ + ': CreateCatalog() returned: ' + catMsg)

        print('catalog filename is ' + catDbName)
        #CreateDatabase(3,  ddlSQL)
        for currentNodeNum in range(1, int(configDict['numnodes']) + 1):
            dbhost = configDict['node' + str(currentNodeNum) + '.hostname']
            dbport = int(configDict['node' + str(currentNodeNum) + '.port'])
            nodeDbName = configDict['node' + str(currentNodeNum) + '.db']

            # print('will connect to node' + str(currentNodeNum) + ' at IP:' + dbhost + ' and port:' + str(dbport))
            t = Thread(target=SendDDLToNode, args=(ddlSQL, dbhost, dbport, currentNodeNum, catDbName, nodeDbName, ))
            t.start()
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runDDL.py cluster.cfg plants.sql\"')



def ParseConfig(clustercfg):
    print(__file__ +': reading config file "' + clustercfg + '"')
    file = open(clustercfg)
    content = file.read()
    configArray = content.split("\n")
    # configList = []
    configDict = {}
    for config in configArray:
        if config:
            c = config.split("=")
            # print (c[0] + ' is ' + c[1])
            configKey = c[0]
            configValue = c[1]
            if(('node' in configKey or 'catalog' in configKey) and 'hostname' in configKey):
                #print('configKey has node hostname')
                nodename = configKey.split(".")[0]
                hostname = configValue.split(":")
                configIP = hostname[0]
                configPort = hostname[1].split("/")[0]
                configDb = hostname[1].split("/")[1]
                '''print('nodename is ' + nodename)
                print('configValue is ' + configValue)
                print('configIP is ' + configIP)
                print('configPort is ' + configPort)
                print('configDb is ' + configDb)'''        
                configDict[nodename + '.port'] = configPort
                configDict[nodename + '.db'] = configDb + '.db'
                configValue = configIP
            configDict[configKey]=configValue
        '''#configList.append(c)[1]
                print (c[0] + '=' + c[1])
                configDict[c[0]] = c[1]'''

    # print ('cfg dictionary is ' + str(configDict))
    print(__file__ +': config file "' + clustercfg + '" read successfully')
    file.close()
    return configDict


if __name__ == '__main__':
    Main()

