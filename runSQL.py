# runSQL.py
import multiprocessing
import pickle
import socket
import sqlite3
import sys
from SQLDriver import SQLDriver
from ClusterDbNode import ClusterDbNode
# import Error

def load_csv(clustercfg, csvfile, sql_driver):
    try:
        print(__file__+': executing runSQL.load_csv')
        #clustercfg = sys.argv[1]
        #csvfile = sys.argv[2]
        #sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)
        #also initializes sql_driver member variables: cat_node, cluster_nodes, cat_node_count
        sql_driver.update_catalog_with_cfg_data()
        csv_tuples = sql_driver.get_tuples_from_csv(csvfile)
        if 'partition.method' in sql_driver.cfg_dict.keys():
            partition_method = sql_driver.cfg_dict['partition.method']
            if(partition_method == 'range'):
                for current_node in sql_driver.cluster_nodes:
                    #get partparams from cfg_dict using nodeid
                    curr_node_id = str(current_node.node_id)
                    curr_param1 = int(sql_driver.cfg_dict['partition.node'+ curr_node_id + '.param1'])
                    curr_param2 = int(sql_driver.cfg_dict['partition.node'+ curr_node_id + '.param2']) 
                    for csv_tuple in csv_tuples:
                        csv_tuple_list = sql_driver.get_fields_from_tuple_string(csv_tuple[0])
                        part_col_value = int(csv_tuple_list[0])
                        #partparam1 < partcol <= partparam2
                        if(curr_param1 < part_col_value <= curr_param2):
                            sql_driver.insert_tuple_into_node_table(current_node, sql_driver.cfg_dict['tablename'], csv_tuple_list)
                            print(__file__ +': insert csv_tuple ' + str(csv_tuple_list) + ' into node ' + str(current_node.node_id))
                        else:
                            print(__file__ +': did not insert csv_tuple ' + str(csv_tuple_list) + ' into node ' + str(current_node.node_id))
            elif(partition_method == 'hash'):
                #print('mod value and send if mod matches node num')
                #create a dictionary where keys are node_id's and values are nodes
                hash_node_dict = sql_driver.get_id_node_dict(sql_driver.cluster_nodes)
                #print('hash_node_dict is: ' + str(hash_node_dict) )
                for csv_tuple in csv_tuples:
                    csv_tuple_list = sql_driver.get_fields_from_tuple_string(csv_tuple[0])
                    #curr_node_id = str(current_node.node_id)
                    part_param1 = int(sql_driver.cfg_dict['partition.param1'])
                    part_col_value = int(csv_tuple_list[0])
                    #( partcol mod partparam1 ) + 1
                    node_id_from_hash = ( part_col_value % part_param1 ) + 1
                    if(node_id_from_hash in hash_node_dict):
                        #print(str(node_id_from_hash) + ' is in hash_node_dict' + ', part_param1 value is ' + hash_node_dict[node_id_from_hash].part_param1)
                        sql_driver.insert_tuple_into_node_table(hash_node_dict[node_id_from_hash], sql_driver.cfg_dict['tablename'], csv_tuple_list)
                    #print('part_param1, part_col_value, node_id_from_hash: ' + str(part_param1) + ',' + str(part_col_value) + ',' + str(node_id_from_hash) )
        else:
            print('send to every node')
            for current_node in sql_driver.cluster_nodes:
                #print('tuples are ' + str(csv_tuples) )
                #sql_driver.insert_csv_tuples_into_node_table(current_node, 'books', csv_tuples)
                sql_driver.insert_csv_tuples_into_node_table(current_node, 'books', csv_tuples)
    #tests(sql_driver)
        # return response_list
    except FileNotFoundError as e:
        print(__file__ + ': ' + str(e) )
    except KeyError as e:
        print(__file__ + ': KeyError Your config file may be missing a value like ' + str(e) )
    except SQLDriver.NodeNumMismatchError as e:
        print(__file__ + ': ' + str(e) )

def sql_is_select_from_on_inner_join_where(sql):
    isSelect = False
    return isSelect

def sql_is_create(sql):
    isInsert = False
    # remove leading whitespace from sql string and split
    sqlArray = sql.lstrip().split(" ")
    if sqlArray[0].upper() == 'CREATE':
        isInsert = True
    return isInsert

def main():
    if(len(sys.argv) >= 3):
        clustercfg = sys.argv[1]
        sql_driver = SQLDriver(__file__, clustercfg)

        #if the config file contains tablename, second commandline argument is treated as the csv file.
        input_file_name = sys.argv[2]
        input_file_string = ''
        with open(input_file_name, 'r') as myfile:
            input_file_string = myfile.read().replace('\n', '')
        print('input_file_string is: '+ input_file_string)

        #if(sql_is_create(input_file_string) ):
        #    print('execute runDDL')
        if('tablename' in sql_driver.cfg_dict):
            print('load csv file')
            csvfile = sys.argv[2]
            load_csv(clustercfg, csvfile, sql_driver)
        #operation is detected from the SQL statement in sqlfile if no tablename in config
        else:
            print('run sql file')
            node_sql = sys.argv[2]
            if( sql_is_select_from_on_inner_join_where(node_sql) is False):
                print('not a join select statement')


                #read sql file as string to be executed on each node using sql_driver.multiprocess_node_sql()
                with open(node_sql, 'r') as myfile:
                    node_sql = myfile.read().replace('\n', '')

                cat_node = sql_driver.get_cat_node_from_cfg()
                cat_node_string = sql_driver.get_node_string_from_cat(cat_node)
                cat_node_tuples = sql_driver.get_tuples_from_csv_string(cat_node_string)
                cluster_nodes = sql_driver.get_nodes_from_tuples(cat_node_tuples)
                sql_driver.multiprocess_node_sql(cluster_nodes, node_sql)
            else:
                print('is a join statement')
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runSQL.py cfg-files/cluster.cfg sql-files/books.sql\")')

if __name__ == '__main__':
    main()
