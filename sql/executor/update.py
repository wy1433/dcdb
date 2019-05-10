from sql.server.context import Context
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from select import ExprNodeExec



class UpdateExec(Executor):
    def __init__(self, ctx=None):
        '''
        @type self.stmt: UpdateStmt
        '''
        super().__init__(ctx)
       
        
    def Execute(self):
        stmt = UpdateStmt()
        BeginExec(self.ctx).Execute()
        
        rowids = ExprNodeExec(self.ctx, stmt.Where).Execute()
        
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
                newValue = stmt.Setlist[i]
                # delete old value
                self.ctx.txn.Set(EncodeKey(col, key), v = None,col= col)
                self.ctx.txn.Set(EncodeIndex(col, key, value),v= None, col= col)
                # insert new value
                self.ctx.txn.Set(EncodeKey(col, key), v = newValue,col= col)
                self.ctx.txn.Set(EncodeIndex(col, key, newValue),v= key, col= col)

        CommitExec(self.ctx).Execute()
        