from sql.server.context import Context
from meta.model import FieldType
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from util.codec.table import *
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
        ctx.status.affectedRows += 1
        
        for i in range(len(stmt.Fields)):
            field = stmt.Fields[i]
            value = stmt.Setlist[i]
            c = store.meta.GetColumnInfoByName(stmt.Table, field)
            dat = c.DataDBName()
            idx = c.IndexDBName()
            row_key = EncodeRowid(rowid)
            row_val = EncodeValue(value, c.fieldType)
            idx_key = EncodeIndexKey(rowid, value, c.fieldType, c.indexType)
            idx_val = EncodeValue(rowid, FieldType.INT)
            txn.Insert(row_key, row_val, dat)
            err = txn.Insert(idx_key, idx_val, idx)
            if err:
                return err

        