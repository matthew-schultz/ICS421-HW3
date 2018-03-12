import sys
from antlr4 import *
from SQLiteParser import SQLiteParser
from SQLiteListener import SQLiteListener

class GetTableSQLiteListener(SQLiteListener) :
    tablename = ''
    # Enter a parse tree produced by SQLiteParser#table_name.
    def enterTable_name(self, ctx:SQLiteParser.Table_nameContext):
        self.tablename = ctx.getText()

    # Exit a parse tree produced by SQLiteParser#table_name.
    def exitTable_name(self, ctx:SQLiteParser.Table_nameContext):
        print("exitTable_name")
