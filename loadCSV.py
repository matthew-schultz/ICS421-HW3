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


def test_insert_dtables(sql_driver):
    insert_dtables = 'insert_dtables.sql'
    with open(insert_dtables, 'r') as myfile:
        data=myfile.read().replace('\n', '')
        # print(__file__ + ': data is ' + data)
        sql_driver.run_sql(data, sql_driver.cfg_dict['tablename'] + '.db')

def test_create_books(sql_driver):
    sql = 'create_books.sql'
    with open(sql, 'r') as myfile:
        data=myfile.read().replace('\n', '')
        # print(__file__ + ': data is ' + data)
        sql_driver.run_sql(data, sql_driver.cfg_dict['tablename'] + '.db')

def test_run(sql_driver):
    dbname = 'mycatdb.db'
    tablename = 'dtables'
    valuename = 'partmtd'
    where_col = 'nodeid'
    where_val = 1
    value = sql_driver.select_value_from_db(dbname, valuename, tablename, where_col, where_val);
    print('value is: ' + value[1])

def test_get_tablename(sql_filename):
    g = GetTablename()
    tablename = g.get_tablename(sql_filename)
    print('tablename test result is ', tablename)

def test_run_sql():
    print(__file__, ': test running runSQL.main()')
    runSQL.main()
    

def tests(sql_driver):
    print(__file__,': running tests')
    test_run_sql()
#    test_get_tablename('books.sql')
#    test_create_books(sql_driver)
#    test_insert_dtables(sql_driver):
#    test_run(sql_driver)


def trim_partmtd(partmtd):
    if(partmtd.startswith('(') ):
        partmtd = partmtd.split('(')[1]
    if(partmtd.endswith(',)\n') ):
        partmtd= partmtd.split(',)')[0]
    return partmtd

def main():
    if(len(sys.argv) >= 3):
        try:
            print(__file__+': executing loadCSV')
            clustercfg = sys.argv[1]
            csvfile = sys.argv[2]
            sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)

            #also initializes sql_driver member variables: cat_node, cluster_nodes, cat_node_count
            sql_driver.update_catalog_with_cfg_data()

            # response_list = []
            csv_tuples = sql_driver.get_tuples_from_csv(csvfile)


            #g = GetTablename()
            #tablename = g.get_tablename(sql_filename)
            #print('tablename test result is ', tablename)

            '''for current_node_num in range(1, int(sql_driver.cfg_dict['numnodes']) + 1):
                if 'partition.method' in sql_driver.cfg_dict.keys():
                    partition_method = sql_driver.cfg_dict['partition.method']
                    # partmtd = trim_partmtd(partmtd)

                    # print('current_node_num is :', current_node_num)
                    if(sql_driver.cfg_dict['partition.method'] == 'range'):
                        print('send if value fits in node range')
                        print('tuples are ' + str(csv_tuples) )
                        
                    elif(sql_driver.cfg_dict['partition.method'] == 'hash'):
                        print('mod value and send if mod matches node num')
                        #sql_driver.partition_hash(tuples)
                else:
                    print('send to every node')
                    print('tuples are ' + str(csv_tuples) )
                    #sql_driver.insert_csv_tuples_into_node_table(node, table_name, csv_tuples)'''

            for current_node in sql_driver.cluster_nodes:
                if 'partition.method' in sql_driver.cfg_dict.keys():
                    partition_method = sql_driver.cfg_dict['partition.method']
                    # partmtd = trim_partmtd(partmtd)

                    # print('current_node_num is :', current_node_num)
                    if(sql_driver.cfg_dict['partition.method'] == 'range'):
                        print('send if value fits in node range')
                        print('tuples are ' + str(csv_tuples) )
                        
                    elif(sql_driver.cfg_dict['partition.method'] == 'hash'):
                        print('mod value and send if mod matches node num')
                        #sql_driver.partition_hash(tuples)
                else:
                    print('send to every node')
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
