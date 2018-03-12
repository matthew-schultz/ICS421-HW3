# runSQL.py
import multiprocessing
import pickle
import socket
import sqlite3
import sys
import SQLDriver
# import Error


def main():
    if(len(sys.argv) >= 3):
        print('executing runSQL')
        clustercfg = sys.argv[1]
        node_sql = sys.argv[2]
        sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)

        #read sql file as string to be executed on each node using sql_driver.multiprocess_node_sql()
        with open(node_sql, 'r') as myfile:
            node_sql = myfile.read().replace('\n', '')

        cat_db_name = sql_driver.get_cat_db()

        sql_driver.multiprocess_node_sql(node_sql, cat_db_name)
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runSQL.py cluster.cfg books.sql\")')

if __name__ == '__main__':
    main()
