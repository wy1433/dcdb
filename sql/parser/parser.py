from sql.server.context import Context
from sql.parser.statement import *
from util.error import *
import re

sysnames = {
    "begin" : BeginStmt,
    'commit': CommitStmt,
    'rollback': RollBackStmt,
    'select' : SelectStmt,
    'insert' : InsertStmt,
    'delete' : DeleteStmt,
    'update' : UpdateStmt,
    }

class Parser():
    def Parser(self, ctx):
        ''' paser a sql string to a statement
        @param ctx:  Context
        @rtype: ErrInvalidSql
        '''
        sql = ctx.sql
#         sql = PreParse(ctx.sql)
        stmt = None
        err = None
        keyword = sql.split()[0].lower()
        if keyword in sysnames:
            v = sysnames[keyword]
            stmt = v(sql)
            err = stmt.Parse()
        else:
            err = ErrInvalidSql
        ctx.stmt = stmt
        ctx.SetErr(err)
        return err

if __name__ == '__main__':
    ctx = Context(sql = "begin")
    p = Parser()
    p.Parser(ctx)
