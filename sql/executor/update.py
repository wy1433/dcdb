from sql.server.context import Context
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from util.codec.table import *
from sql.executor.select import ExprNodeExec, PointGetExec, BatchGetExec



class UpdateExec(Executor):

    def __init__(self, ctx=None):
        '''
        @param ctx: Context
        '''
        super(UpdateExec, self).__init__(ctx)
        
    def Execute(self):        
        ctx = self.ctx  # : :type ctx: Context
        stmt = ctx.stmt  # : :type stmt: UpdateStmt
        store = ctx.Store()
        
        # 1. get rowids from where
        rowids, err = ExprNodeExec(ctx, stmt.Where).Execute() #: :type rowids: set(str)
        if err:
            return err
        
        
        # 2. set to sorted list 
        rids = list(rowids)
        rids.sort()
        
        txn = ctx.Txn()
        ctx.status.affectedRows += len(rids)
        
        # 3. get field's key/value by rids, 
        # and delete the data and idx at the same time
        # and insert the new data and idx at last.
        t = store.meta.GetTableInfoByName(stmt.Table)
        
        for i in range(len(stmt.Fields)):
            field = stmt.Fields[i]
            value = stmt.Setlist[i]
            c = store.meta.GetColumnInfoByName(stmt.Table, field)
        
  
            dat = c.DataDBName()
            idx = c.IndexDBName()
            d, err = BatchGetExec(self.ctx, field, rids).Execute() #: :type d: dict(str, str)
            if err:
                return ErrExecutor
            for row_key, encode_value in d.iteritems():
                rowid = DecodeRowid(row_key)
                
                # remove old data
                old_value = DecodeValue(encode_value, c.fieldType)
                old_idx_key = EncodeIndexKey(rowid, old_value, c.fieldType, c.indexType)
                txn.Delete(row_key, dat)
                txn.Delete(old_idx_key, idx)
                
                # insert new data
#                 row_key = EncodeRowid(rowid)
                row_val = EncodeValue(value, c.fieldType)
                idx_key = EncodeIndexKey(rowid, value, c.fieldType, c.indexType)
                idx_val = EncodeValue(rowid, FieldType.INT)
                txn.Insert(row_key, row_val, dat)
                err = txn.Insert(idx_key, idx_val, idx)
                if err:
                    return err