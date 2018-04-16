import sys
from antlr4 import *
from SQLiteLexer import SQLiteLexer
# from PrintSQLiteListener import PrintSQLiteListener
from IsSelectSQLiteListener import IsSelectSQLiteListener
from SQLiteParser import SQLiteParser

class IsSelect() :
    # tablename = ''

    def __init__(self):
        print()

    def is_select(self, filename):
        input = FileStream(filename)
        lexer = SQLiteLexer(input)
        stream = CommonTokenStream(lexer)
        parser = SQLiteParser(stream)
        tree = parser.sql_stmt()
    #    output = open("sqlOutput.txt","w")        
        SQLite = IsSelectSQLiteListener()
        walker = ParseTreeWalker()
        walker.walk(SQLite, tree)
        return SQLite.isSelect
'''
    def main(argv):
        input = FileStream(argv[1])
        tablename = get_tablename(input)
        print(tablename)   
    #    output.close()      

    def test(argv):
        print("this is a test")
     
    if __name__ == '__main__':
        if(len(sys.argv) >= 2):
            main(sys.argv)
        else:
            print(__file__ + ': ERROR need at least 2 arguments to run properly (e.g. \"python3 antlr.py books.sql\"')
    #    test(sys.argv)'''
