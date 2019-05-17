from sql.server.context import Context
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from select import ExprNodeExec



class DeleteExec(Executor):
    def __init__(self, ctx=None):
        '''
        @type self.stmt: DeleteStmt
        '''
        super().__init__(ctx)
       
        
    def Execute(self):
        ctx = self.ctx #: :type ctx: Context
        stmt = ctx.stmt #: :type stmt: InsertStmt
        store = ctx.Store()
        txn = ctx.Txn()
        
        rowids = ExprNodeExec(self.ctx, self.stmt.Where).Execute()
        
        rs  = list()
        if len(rowids) == 0:
            pass
        elif len(rowids) == 1:
            f = PointGetExec(self.ctx, 'row_id').Execute()
            rs.append(f)
        else:
            f = BatchGetExec(self.ctx, 'row_id').Execute()
            rs.append(f)
        
        for col in stmt.Table.Columns:
            for i in range(len(rowids)):
                key = rowids[i]
                value = rs[i]
            self.ctx.txn.Set(EncodeKey(col, key), v = None,col= col)
            self.ctx.txn.Set(EncodeIndex(col, key, value),v= None, col= col)

        CommitExec(self.ctx).Execute()
        