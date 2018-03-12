import sys
from antlr4 import *
from SQLiteParser import SQLiteParser
from SQLiteListener import SQLiteListener

class PrintSQLiteListener(SQLiteListener) :
    # Enter a parse tree produced by SQLiteParser#table_name.
    def enterTable_name(self, ctx:SQLiteParser.Table_nameContext):
        print(ctx.getText())

    # Exit a parse tree produced by SQLiteParser#table_name.
    def exitTable_name(self, ctx:SQLiteParser.Table_nameContext):
        print("exit")
