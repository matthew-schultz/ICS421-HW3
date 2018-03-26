# runSQL.py
import multiprocessing
import pickle
import socket
import sqlite3
import sys
from SQLDriver import SQLDriver
from ClusterDbNode import ClusterDbNode
# import Error


def main():
    if(len(sys.argv) >= 3):
        '''node1 = ClusterDbNode(db_name="mydb1", host="172.17.0.3", port="5000", part_col='id', part_param1='1', part_param2='2', part_mtd='99', node_id='111')
        print('node db_name is ', node1.db_name)
        print('node host is ', node1.host)
        print('node port is ', node1.port)
        print('node part_col is ', node1.part_col)
        print('node part_param1 is ', node1.part_param1)
        print('node part_param2 is ', node1.part_param2)
        print('node part_mtd is ', node1.part_mtd)
        print('node node_id is ', node1.node_id)'''
        print('executing runSQL')
        clustercfg = sys.argv[1]
        node_sql = sys.argv[2]
        sql_driver = SQLDriver(__file__, clustercfg)

        #read sql file as string to be executed on each node using sql_driver.multiprocess_node_sql()
        with open(node_sql, 'r') as myfile:
            node_sql = myfile.read().replace('\n', '')

        # cat_db_name = sql_driver.get_cat_db()

        # sql_driver.multiprocess_node_sql(node_sql)
        cat_node = ClusterDbNode(db_name="mycatdb.db", host="172.17.0.3", port="5000", part_col='id', part_param1='1', part_param2='2', part_mtd='99', node_id='111')
        sql_driver.get_node_string_from_cat(cat_node)
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runSQL.py cfg-files/cluster.cfg sql-files/books.sql\")')

if __name__ == '__main__':
    main()
