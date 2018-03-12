import sys
from antlr4 import *
from SQLiteLexer import SQLiteLexer
# from PrintSQLiteListener import PrintSQLiteListener
from GetTableSQLiteListener import GetTableSQLiteListener
from SQLiteParser import SQLiteParser
 
def get_table_name(input):
    lexer = SQLiteLexer(input)
    stream = CommonTokenStream(lexer)
    parser = SQLiteParser(stream)
    tree = parser.sql_stmt()
#    output = open("sqlOutput.txt","w")
    
#    SQLite = PrintSQLiteListener()
    SQLite = GetTableSQLiteListener()
    walker = ParseTreeWalker()
    walker.walk(SQLite, tree)
    return SQLite.tablename

def main(argv):
    input = FileStream(argv[1])
    tablename = get_table_name(input)
    print(tablename)   
#    output.close()      

def test(argv):
    print("this is a test")
 
if __name__ == '__main__':
    if(len(sys.argv) >= 2):
        main(sys.argv)
    else:
        print(__file__ + ': ERROR need at least 2 arguments to run properly (e.g. \"python3 antlr.py books.sql\"')
#    test(sys.argv)
