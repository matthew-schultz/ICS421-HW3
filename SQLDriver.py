# runSQL.py
# import configparser
import csv
import multiprocessing
import pickle
import socket
import sqlite3
import os, sys, inspect
from antlr4 import *
from ClusterDbNode import ClusterDbNode
from io import StringIO
# import Error

# https://stackoverflow.com/questions/279237/import-a-module-from-a-relative-path
# realpath() will make your script run, even if you symlink it :)
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

# Use this if you want to include modules from a subfolder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"antlr-files")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

# Use this if you want to include modules from a subfolder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"sql-files")))
if cmd_subfolder not in sys.path:
    sys.path.insert(0, cmd_subfolder)

from GetTablename import GetTablename

class NodeNumMismatchError(Exception):
    def __init__(self, message, errors):

        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = 'numnodes in clustercfg does not match number of nodes in dtables'

class SQLDriver:
    '''
    Parameters
    caller_file : the name of the file that instantiated this class
    clustercfg  : the name of the config file for the database cluster

    this function also creates a dictionary from the database cluster config file
    '''
    def __init__(self, caller_file, clustercfg):
        self.caller_file = caller_file
        self.partcol_dict = {}
        self.partcol_dict['range'] = 1
        self.partcol_dict['hash'] = 2
        if clustercfg is not None:
            self.cfg_dict = self.get_cfg_dict(clustercfg)
            # print(caller_file + ': cfg_dict is: ' + str(self.cfg_dict) )
            '''print('dict fields :')
            for x in self.cfg_dict:
                print(x,':',self.cfg_dict[x])'''

    def create_catalog(self, dbname):
        sqlConn = sqlite3.connect(dbname)
        c = sqlConn.cursor()
        # cat_sql = 'DROP TABLE dtables;\n'
        cat_sql = '''CREATE TABLE IF not exists dtables(tname char(32), 
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
            c.execute(cat_sql)
        except Error:
            tableCreatedMsg = 'failure'
        else:
            tableCreatedMsg = 'success'
        sqlConn.commit()
        sqlConn.close()
        return tableCreatedMsg

    def get_cat_db(self):
        return self.cfg_dict['catalog.db']

    def get_cfg_dict(self, clustercfg):
        print(self.caller_file +': reading config file "' + clustercfg + '"')
        file = open(clustercfg)
        content = file.read()
        config_array = content.split("\n")
        config_dict = {}
        for config in config_array:
            if config:
                c = config.split("=")
                # print (c[0] + ' is ' + c[1])
                config_key = c[0]
                configValue = c[1]
                if(('node' in config_key or 'catalog' in config_key) and 'hostname' in config_key):
                    #print('config_key has node hostname')
                    nodename = config_key.split(".")[0]
                    hostname = configValue.split(":")
                    configIP = hostname[0]
                    configPort = hostname[1].split("/")[0]
                    configDb = hostname[1].split("/")[1]
                    '''print('nodename is ' + nodename)
                    print('configValue is ' + configValue)
                    print('configIP is ' + configIP)
                    print('configPort is ' + configPort)
                    print('configDb is ' + configDb)'''        
                    config_dict[nodename + '.port'] = configPort
                    config_dict[nodename + '.db'] = configDb #+ '.db'
                    configValue = configIP
                config_dict[config_key]=configValue
        print(self.caller_file +': config file "' + clustercfg + '" read successfully')
        file.close()
        return config_dict


    def get_table_name(self, data):
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

    def update_catalog_with_cfg_data(self):
        num_nodes = int(self.cfg_dict['numnodes'] )

        self.cat_node = self.get_cat_node_from_cfg()
        cat_node_string = self.get_node_string_from_cat(self.cat_node)
        cat_node_tuples = self.get_tuples_from_csv_string(cat_node_string)
        self.cluster_nodes = self.get_nodes_from_tuples(cat_node_tuples)
        self.cluster_node_count = len(self.cluster_nodes)
        if num_nodes == self.cluster_node_count:
            for current_node_num in range(1, num_nodes + 1):    
    #            print('current_node_num in update_catalog is ',current_node_num)
                statement_to_run = ''
                try:
                    if(self.check_catalog_if_node_exists(current_node_num) == '1'):
                        statement_to_run = self.build_catalog_update_statement(current_node_num)
                        #print('''self.caller_file +''' 'statement_to_run, cat_node.host, cat_node.port, cat_node.db_name contents are: ',statement_to_run, self.cat_node.host, self.cat_node.port, self.cat_node.db_name)
                        self.send_node_sql(statement_to_run, self.cat_node.host, int(self.cat_node.port), self.cat_node.db_name)
                    else:
                        raise NodeNumMismatchError(self.caller_file +': NodeNumMismatchError node with nodeid:' + str(current_node_num) + ' was not found in catalog', None)
                except NodeNumMismatchError as e:
                    print(str(e) )
                print(self.caller_file +': updating catalog node '+ str(current_node_num) + ' with sql statement ' + statement_to_run)        
                dbname = self.cfg_dict['catalog.db']
                # self.run_sql(statement_to_run, dbname)
        else:
            raise NodeNumMismatchError('NodeNumMismatchError numnodes in cfg file does not match number of nodes in dtables', None)

# this function takes the int current_node_num, creates variables for each column in dtables,
# stores the associated cfg_dict key in the variable
# adds a 'colname=colvalue,' section to the update statement string if it exists in the cfg_dict
    def build_catalog_update_statement(self, current_node_num):
        statement = 'update dtables set '
        #check if partition.method exists
        #try
        if('partition.method' not in self.cfg_dict.keys() or (self.cfg_dict['partition.method'] != 'range' and self.cfg_dict['partition.method'] != 'hash') ):
            #print ('yay it\'s not in keys')
            #tablename=books
            statement += 'tname="' + self.cfg_dict['tablename'] + '",'
            #partition.method=hash
            statement += 'partmtd=0,'
            #null fields and end
            statement += 'partparam1=NULL, partparam2=NULL, partcol=NULL'
            statement += ' where nodeid=' + str(current_node_num) + ';'
        elif(self.cfg_dict['partition.method'] == 'range'):
            #tablename=books
            statement += 'tname="' + self.cfg_dict['tablename'] + '",'
            #partition.method=range
            statement += 'partmtd=1,'
            #partition.column=age
            statement += 'partcol="' + self.cfg_dict['partition.column'] + '",'
            #partition.node1.param1=1
            partparam1 = 'partition.node' + str(current_node_num) + '.param1'
            statement += 'partparam1="' + self.cfg_dict[partparam1] + '",'
            #partition.node1.param2=10
            partparam2 = 'partition.node' + str(current_node_num) + '.param2'
            statement += 'partparam2="' + self.cfg_dict[partparam2] + '",'
            #null fields and end
            statement += 'partcol=NULL'
            statement += ' where nodeid=' + str(current_node_num) + ';'
        elif(self.cfg_dict['partition.method'] == 'hash'):
            #tablename=books
            statement += 'tname="' + self.cfg_dict['tablename'] + '",'
            #partition.method=hash
            statement += 'partmtd=2,'
            #partition.column=age
            statement += 'partcol="' + self.cfg_dict['partition.column'] + '",'
            #partition.param1=2
            statement += 'partparam1="' + self.cfg_dict['partition.param1'] + '",'
            #null fields and end
            statement += 'partparam2=NULL'
            statement += ' where nodeid=' + str(current_node_num) + ';'            
        return statement

#return string '1' if row for node exists, return '0' if it does not
    def check_catalog_if_node_exists(self, nodenum):
        catalog_statement = 'SELECT EXISTS(SELECT 1 FROM dtables WHERE nodeid='+str(nodenum)+')';
        catdbname = self.cfg_dict['catalog.db']
#        print('catdbname is' + catdbname)
        response_arr = self.run_sql(catalog_statement, catdbname)
        #print('check_catalog_if_node_exists response[1] is', str(response_arr[1]) )
        # trim responses like '(1,)' to '1'
        response = self.trim_sql_response(response_arr[1]) 
        #print('check_catalog_if_node_exists trimmed response is', response)
        return response

#trim sql responses like '(1,)' to '1'
    def trim_sql_response(self, sql):
        if(sql.startswith('(') ):
            sql = sql.split('(')[1]
        if(sql.endswith(',)\n') ):
            sql= sql.split(',)')[0]
        return sql

    def trim_parentheses(self, string):
        if(string.startswith('(') ):
            string = string.split('(')[1]
        if(string.endswith(')') ):
            string= string.split(')')[0]
        return string

    def get_node_dict_from_catalog(self, nodenum):
        node_dict = []        
        # partmtd_sql = 'SELECT partmtd from dtables WHERE nodeid=' + nodenum + ';'
        # partmtd = select_value_from_db(self, dbname, tablename, valuename, where_col, where_val)
        # node_dict.append(partmtd)

        return []

    '''
    Need to include '' in where_val arg string if that db column is of string type
    Arguments are in sql order "SELECT valuename FROM tablename WHERE where_col = where_val;"
    '''
    def select_value_from_db(self, dbname, valuename, tablename, where_col, where_val):
        value = ''
        select_sql = 'SELECT ' + valuename + ' FROM ' + tablename + ' WHERE ' \
        + where_col + '= ' + str(where_val) + ';'

        sql_result = self.run_sql(select_sql, dbname)
        if(sql_result[0] == 'success'):
            value = sql_result[1]
        print('select was ', value)
        return value

    # return 0 if partition method field does not exist
    def get_partmtd(self):
        if('partition.method' in self.cfg_dict):
            partmtd_string = self.cfg_dict['partition.method']
            if(partmtd_string == 'range'):
                return 1
            if(partmtd_string == 'hash'):
                return 2
            else:
                print ('partition.method was found in cfg_dict but did not match "range" or "hash"')
                return 0
        else:
            return 0

    def get_tuples_from_csv(self, csv_filename):
        tuples = []
        with open(csv_filename, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter='\n')
            for row in reader:
                tuples.append(row)
        return tuples

    def get_tuples_from_csv_string(self, csv_string):
        tuples = []
        # with open(csv_filename, newline='') as csvfile:
        # reader = csv.reader(csv_string, delimiter='\n')
        for line in csv_string.splitlines():
            tuples.append(line)
        return tuples

    def get_fields_from_tuple_string(self, tuple_string):
        fields = []
        for field in tuple_string.split(','):
            field = field.strip(' ')
            field = field.strip('\'')
            fields.append(field)
            #print('field is :', field)
        return fields
        

    def get_nodes_from_tuples(self, node_tuples):
        nodes = []
        i = 1
        for node_tuple in node_tuples:
            node_tuple = self.trim_parentheses(node_tuple)
            fields = self.get_fields_from_tuple_string(node_tuple)
            node_url = fields[0]
            host_and_port = node_url.split(":")
            host = host_and_port[0]
            port = host_and_port[1].split("/")[0]
            db_name = host_and_port[1].split("/")[1]
            part_mtd = fields[1]
            node_id = fields[2]
            part_col = fields[3]
            part_param1 = fields[4]
            part_param2 = fields[5]
            node = ClusterDbNode(db_name=db_name, host=host, port=port, part_col=part_col, part_param1=part_param1, part_param2=part_param2, part_mtd=part_mtd, node_id=i)
            nodes.append(node)
            #print('get_nodes_from_tuples current node is: ', node_tuple)
            #print('fields are ', str(fields) )
            #print('node dbname,host,port,partcol,partparam1,partparam2,partmtd,nodeid is ', node.db_name, node.host, node.port, node.part_col, node.part_param1, node.part_param2, node.part_mtd, node.node_id)
            i+=1
        return nodes

    def partition_all(self, tuples):
        for sql_tuple in tuples:
            # insert = self.insert_tuple(self.cfg_dict['tablename'], sql_tuple)
            insert_sql = 'INSERT into ' + self.cfg_dict['tablename'] + ' VALUES(' + sql_tuple[0] + ');'
            print(': insert is ',insert_sql)
            # print('idx is ' + str(idx))

    #tuple must be a list of values in a row in the given table; builds a sql insert statement            
    def insert_tuple_into_node_table(self, node, table_name, tuple_list):
        # print('tup is' + sql_tuple[0])
        insert_sql = 'INSERT into ' + table_name + ' VALUES(' 
        for column_iterator in range(len(tuple_list) ):
            insert_sql += tuple_list[column_iterator]
            if(column_iterator < len(tuple_list) - 1):
                 insert_sql += ','
        insert_sql += ');'
        #node.host, node.port, node.db_name
        self.send_node_sql(insert_sql, node.host, int(node.port), node.db_name)
        print(self.caller_file + ': insert_sql is ' + insert_sql)
        return insert_sql

    def insert_csv_tuples_into_node_table(self, node, table_name, csv_tuples):
        for csv_tuple in csv_tuples:
            print('csv_tuple is: ' + str(csv_tuple) )
            self.insert_tuple_into_node_table(node, table_name, csv_tuple)
    
    #return a dictionary with node id's as keys and ClusterDbNodes as values
    def get_id_node_dict(self, cluster_nodes):
        hash_node_dict = {}
        for cluster_node in cluster_nodes:
            hash_node_dict[cluster_node.node_id] = cluster_node
        return hash_node_dict

    def get_cat_node_from_cfg(self):
        cat_dbname = self.cfg_dict['catalog.db']
        cat_host = self.cfg_dict['catalog.hostname']
        cat_port = self.cfg_dict['catalog.port']        
        #cat_node = ClusterDbNode(db_name=cat_dbname, host="172.17.0.2", port="5000", part_col='id', part_param1='1', part_param2='2', part_mtd='99', node_id='112')
        cat_node = ClusterDbNode(db_name=cat_dbname, host=cat_host, port=cat_port)
        '''print('cat_dbname is: ',cat_dbname)
        print('cat_host is: ',cat_host)
        print('cat_port is: ',cat_port)
        print('cat_node.get_insert_sql_string is: ',cat_node.get_insert_sql_string() )'''
        return cat_node

    def multiprocess_node_sql(self, nodes, node_sql):   
        # create a pool of resources, allocating one resource for each node
        try:
            pool = multiprocessing.Pool(len(nodes) )
            node_sql_response = []
            for current_node in nodes:
                db_host = current_node.host
                db_port = current_node.port
                db_name = current_node.db_name
                #print('current node info is ',db_host,db_port,db_name)
                node_sql_response.append(pool.apply_async(self.send_node_sql, (node_sql, db_host, int(db_port), db_name, ) ) )
            for current_node in range(len(nodes) ):
                node_sql_response.pop(0).get()
        except ValueError as e:
            print(str(e) + '\nThe catalog table may have <1 rows; >=1 rows are required')
        except EOFError as e:
            print(str(e) + '\nThe target table may too many rows to send in a pickle')

    def get_node_string_from_cat(self, cat_node):
        cat_table_name = 'dtables'
        cat_sql = 'select nodeurl,partmtd,nodeid,partcol,partparam1,partparam2 from ' + cat_table_name
        cat_sql_response = self.send_node_sql(cat_sql, cat_node.host, int(cat_node.port), cat_node.db_name)
        if(len(cat_sql_response) > 0):
            #print(self.caller_file,': get_node_string_from_cat cat_sql_response:\n',cat_sql_response[1])
            return cat_sql_response[1]
        else:
            #print(self.caller_file + ': get_node_string_from_cat cat_sql_response: Empty')
            return ''


    def count_rows_in_table(self, tablename, dbname):
        num_nodes_sql = "select count(*) from " + tablename
        results = self.run_sql(num_nodes_sql, dbname)
        count_string = results[1]
        count = int(self.trim_sql_response(count_string) )
        # print ('count_rows_in_table: ' + str(count) )
        return count


    def run_sql(self, sql, dbname):
        print(self.caller_file + ': executing sql statement ' + sql) 
        try:
            sqlConn = sqlite3.connect(dbname)
            c = sqlConn.cursor()
            result = ''
            for row in c.execute(sql):
                # print('str is ' + str(row))
                result += str(row) + '\n'
            sqlConn.commit()
            sqlConn.close()
            # create sql_result to store if sql was success and rows
            sql_result = []
            sql_result.append('success')
            sql_result.append(result)
            # print(self.caller_file + ': returning results of sql statement ' + sql) 
            return sql_result
        except sqlite3.IntegrityError as e:
            print(e)
            sql_result = []
            sql_result.append('failure')
            sql_result.append('')
            return sql_result
        except sqlite3.OperationalError as e:
            print(e)
            sql_result = []
            sql_result.append('failure')
            sql_result.append('')
            return sql_result

    def send_node_sql(self, node_sql, dbhost, dbport, node_db):
        print(self.caller_file + ': connecting to host ' + dbhost)
        my_socket = socket.socket()
        try:
            my_socket.connect((dbhost, dbport))
            req_to_pickle = []
            req_to_pickle.append(node_db)
            req_to_pickle.append(node_sql)
            data_string = pickle.dumps(req_to_pickle)
            print(self.caller_file + ': send pickled data_array "' + '[%s]' % ', '.join(map(str, req_to_pickle)) + '"')   
            # my_socket.send(packet.encode())
            my_socket.send(data_string)
            data = my_socket.recv(1024)
            data_arr = pickle.loads(data)
            # return from Main() if no data was received
            if not data_arr:
                return
            # data_arr = pickle.loads(data)
            # print(self.caller_file + ': recv array ' + repr(data_arr))
            dbfilename = data_arr[0]
            print(self.caller_file + ': recv msg ' + data_arr[0] + ' from host ' + dbhost)
            print(self.caller_file + ': recv sql rows' + data_arr[1] + ' from host ' + dbhost)
            my_socket.close()
            return data_arr
        except OSError:
            print(self.caller_file + ': failed to connect to host ' + dbhost)
            my_socket.close()
            return []
#        my_socket.close()


    def table_is_created(self, sql):
        isInsert = False
        # remove leading whitespace from sql string and split
        sqlArray = sql.lstrip().split(" ")
        if sqlArray[0].upper() == 'CREATE':
            isInsert = True
        return isInsertpartparam1
