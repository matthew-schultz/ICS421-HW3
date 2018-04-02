#loadCSV.py

# import sys
import os, sys, inspect
import SQLDriver
from GetTablename import GetTablename
import runSQL

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

def main():
    if(len(sys.argv) >= 3):
        try:
            print(__file__+': executing loadCSV')
            clustercfg = sys.argv[1]
            csvfile = sys.argv[2]
            sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)
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
                            #insert_tuple_into_node_table()
                        print('part_param1, part_col_value, node_id_from_hash: ' + str(part_param1) + ',' + str(part_col_value) + ',' + str(node_id_from_hash) )
                    #sql_driver.partition_hash(tuples)
                        #sql_driver.insert_csv_tuples_into_node_table(current_node, 'books', csv_tuples)
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
    else:
        print(__file__ + ': ERROR need at least 2 arguments to run properly (e.g. \"python3 loadCSV.py cluster.cfg books.csv\"')

if __name__ == '__main__':
    main()
