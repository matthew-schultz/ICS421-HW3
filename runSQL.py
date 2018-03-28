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
        clustercfg = sys.argv[1]
        node_sql = sys.argv[2]
        sql_driver = SQLDriver(__file__, clustercfg)

        #read sql file as string to be executed on each node using sql_driver.multiprocess_node_sql()
        with open(node_sql, 'r') as myfile:
            node_sql = myfile.read().replace('\n', '')

        cat_node = sql_driver.get_cat_node_from_cfg()
        cat_node_string = sql_driver.get_node_string_from_cat(cat_node)
        cat_node_tuples = sql_driver.get_tuples_from_csv_string(cat_node_string)
        cluster_nodes = sql_driver.get_nodes_from_tuples(cat_node_tuples)
        sql_driver.multiprocess_node_sql(cluster_nodes, node_sql)        
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runSQL.py cfg-files/cluster.cfg sql-files/books.sql\")')

if __name__ == '__main__':
    main()
