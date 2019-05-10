from sql.server.context import Context
from meta.model import FieldType
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from util.codec.table import *
from select import ExprNodeExec
from mylog import logger


class InsertExec(Executor):
    def __init__(self, ctx=None):
        '''
        @param ctx: Context
        '''
        super(InsertExec,self).__init__(ctx)
       
    def Execute(self):
        ctx = self.ctx #: :type ctx: Context
        stmt = ctx.stmt #: :type stmt: InsertStmt
        store = ctx.Store()
        txn = ctx.Txn()

        table_info = store.meta.GetTableInfoByName(stmt.Table)
        rowid = store.meta.GetRowID(table_info.id)
        ctx.status.lastInsertId = rowid
        
        for i in range(len(stmt.Fields)):
            field = stmt.Fields[i]
            value = stmt.Setlist[i]
            c = store.meta.GetColumnInfoByName(stmt.Table, field)
            fieldType = c.fieldType
            dat = c.DataDBName()
            idx = c.IndexDBName()
            row_key = EncodeRowid(rowid)
            row_val = EncodeValue(value, fieldType)
            idx_key = EncodeIndexKey(rowid, value, fieldType)
            idx_val = EncodeValue(rowid, FieldType.INT)
            ctx.txn.Set(row_key, row_val, dat)
            ctx.txn.Set(idx_key, idx_val, idx)

        