import sys
from antlr4 import *
from SQLiteParser import SQLiteParser
from SQLiteListener import SQLiteListener

class IsSelectSQLiteListener(SQLiteListener) :
    isSelect = False
    # Enter a parse tree produced by SQLiteParser#table_name.
    #def enterTable_name(self, ctx:SQLiteParser.Table_nameContext):
    #    self.tablename = ctx.getText()

    # Exit a parse tree produced by SQLiteParser#table_name.
    #def exitTable_name(self, ctx:SQLiteParser.Table_nameContext):
    #    print("exitTable_name")

    # Enter a parse tree produced by SQLiteParser#Select_stmtContext.
    def enterIsSelect(self, ctx:SQLiteParser.Select_coreContext):
        self.isSelect = True

    # Exit a parse tree produced by SQLiteParser#Select_stmtContext.
    def exitIsSelect(self, ctx:SQLiteParser.Select_coreContext):
        print("exitIsSelect")
        self.isSelect = True
