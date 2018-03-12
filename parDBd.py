#parDBD.py
import pickle
import socket
import sqlite3
import sys
import SQLDriver
#import sqlite3.OperationalError


# Create table
def CreateTable(dbfilename, ddlSQL):
    sqlConn = sqlite3.connect(dbfilename)
    c = sqlConn.cursor()
    tableCreatedMsg = ''
    try:
       c.execute(ddlSQL)
    except sqlite3.OperationalError as e:
       print(e)
       tableCreatedMsg = 'failure'
    else:
        tableCreatedMsg = 'success'
    c.close()
    sqlConn.commit()
    sqlConn.close()
    return tableCreatedMsg


def Main():
    if(len(sys.argv) >= 3):
        sql_driver = SQLDriver.SQLDriver(__file__, None)
        host = sys.argv[1]
        #host = ''
        port = int(sys.argv[2])

        mySocket = socket.socket()
        mySocket.bind((host,port))
        mySocket.listen(1)
        runDDLConn, addr = mySocket.accept()
        print (__file__ + ': Connection from ' + str(addr))
        data = runDDLConn.recv(1024)
        # return from Main() if no data was received
        if not data:
            return
        data_arr = pickle.loads(data)
        print(__file__ + ': Received' + repr(data_arr))

        dbfilename = data_arr[0]
        print(__file__ + ': dbfilename is ' + dbfilename)
        ddlSQL = data_arr[1]
        print (__file__ + ': ddlSQL is ' + ddlSQL)

        sql_response = sql_driver.run_sql(ddlSQL, dbfilename)

        # response = ''#CreateTable(dbfilename, ddlSQL)
        # if(sql_response[0] != 'failure'):
            
        print('parDBd: sql statement ' + sql_response[0])
        print('parDBd: send response "' + str(sql_response) +  '" for sql "' + str(ddlSQL) + '"')

        data_string = pickle.dumps(sql_response)

        runDDLConn.send(data_string)
        runDDLConn.close()
        mySocket.close()
    else:
        print("parDBd: ERROR need at least 3 arguments to run properly (e.g. \"python3 parDBd.py 172.17.0.2 5000\"")


if __name__ == '__main__':
    try:
        Main()
    except OSError as e:
        print('failed due to OSError; please retry in a minute\n' + str(e))
