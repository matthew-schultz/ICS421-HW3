class ClusterDbNode:
    # requires keyword arguments (kwargs) for database name, host, and port
    # may also optionally include kwargs for partition column, two partition parameters, and a node id
    def __init__(self, *args, **kwargs):
        self.db_name = kwargs['db_name']
        self.host = kwargs['host']
        self.port = kwargs['port']
        if('part_col' in kwargs):
            self.part_col = kwargs['part_col']
        if('part_param1' in kwargs):
            self.part_param1 = kwargs['part_param1']
        if('part_param2' in kwargs):
            self.part_param2 = kwargs['part_param2']
        if('part_mtd' in kwargs):
            self.part_mtd = kwargs['part_mtd']
        if('node_id' in kwargs):
            self.node_id = kwargs['node_id']
        if('table_name' in kwargs):
            self.table_name = kwargs['table_name']

    # 'books', 'com.ibm.db2.jcc.DB2Driver','10.0.0.2:5000/mydb1','db2inst1', 'mypasswd', 1,1,'age',1,2
    # table_name, nodedriver, host:port/db_name, nodeuser, nodepasswd, partmtd, node_id, part_col, part_param1, partparam2 
    def get_insert_sql(self):
        sql = 'INSERT into dtables VALUES('
        try: #table_name
            sql += '"' + self.table_name + '"' +  ','
        except AttributeError:
            sql += 'NULL,'
        #nodedriver
        sql += 'NULL,'
        try: #host:port/db_name
            sql +=  '"' + self.host + ':' + self.port + '/' + self.db_name + '"' + ','
        except AttributeError:
            sql += 'NULL'
        # nodeuser, nodepasswd
        sql += 'NULL,NULL,'
        try: # partmtd
            sql += str(self.part_mtd) + ','
        except AttributeError:
            sql += 'NULL,'
        try: # node_id
            sql += str(self.node_id) + ','
        except AttributeError:
            sql += 'NULL,'
        try: # part_col
            sql += '"' + self.part_col + '"' + ','
        except AttributeError:
            sql += 'NULL,'
        try: # part_param1
            sql += '"' + self.part_param1 + '"' +  ','
        except AttributeError:
            sql += 'NULL,'
        try: # part_param2
            sql += '"' + self.part_param2 + '"'
        except AttributeError:
            sql += 'NULL'
        sql += ');'
        return sql
