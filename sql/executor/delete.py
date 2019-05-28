from sql.server.context import Context
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from util.codec.table import *
from sql.executor.select import ExprNodeExec, PointGetExec, BatchGetExec



class DeleteExec(Executor):

    def __init__(self, ctx=None):
        '''
        @param ctx: Context
        '''
        super(DeleteExec, self).__init__(ctx)
        
    def Execute(self):        
        ctx = self.ctx  # : :type ctx: Context
        stmt = ctx.stmt  # : :type stmt: DeleteStmt
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
        
        # 3. get field's key/value by rids, and delete the data and idx at the same time
        t = store.meta.GetTableInfoByName(stmt.Table)
        
        for c in t.columns:
            field = c.name
            if field == 'row_id':
                continue
            dat = c.DataDBName()
            idx = c.IndexDBName()
            d, err = BatchGetExec(self.ctx, field, rids).Execute() #: :type d: dict(str, str)
            if err:
                return ErrExecutor
            for row_key, encode_value in d.iteritems():
                rowid = DecodeRowid(row_key)
                value = DecodeValue(encode_value, c.fieldType)
                idx_key = EncodeIndexKey(rowid, value, c.fieldType, c.indexType)
                txn.Delete(row_key, dat)
                txn.Delete(idx_key, idx)        