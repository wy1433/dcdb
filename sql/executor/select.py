from sql.server.context import Context
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *


def HasIndex(col):
    return True

def IndexKey(col, key):
    return ""

class ColumnScanExec(Executor):
    def __init__(self, ctx=None, col = None, start = None, end = None):
        super().__init__(ctx)
    def Execute(self):
        '''
        @todo: implemented. scan + filter
        '''
        pass

class IndexScanExec(Executor):
    def __init__(self, ctx=None, col = None, start = None, end = None):
        super().__init__(ctx)
        self.col = col
        self.start = IndexKey(col, start)
        self.end = IndexKey(col, end)
        
    def Execute(self):
        return self.ctx.txn.Scan(self.start, self.end, limit=None, col=self.col)

class RangeScanExec(Executor):
    def __init__(self, ctx=None, col = None, start = None, end = None):
        super().__init__(ctx)
        self.col = col
        self.start = start
        self.end = end
        
    def Execute(self):
        if HasIndex(self.col):
            e = IndexScanExec(self.ctx, self.col, self.start, self.end)
        else:
            e = ColumnScanExec(self.ctx, self.col, self.start, self.end)
        return e.Execute()    
        


class ConditionExec(Executor):
    def __init__(self, ctx=None, stmt = None):
        '''
        @type stmt: ConditionExpr
        '''
        super().__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        if self.stmt.ConditionType == ConditionType.ConditionTypeRangeScan:
            return RangeScanExec(self.ctx, self.stmt.column, start= self.stmt.start, end=self.stmt.end).Execute()
        else:
            return ErrExecutor

class UnionExec(Executor):
    def __init__(self, ctx=None, stmt = None):
        '''
        @type stmt: UnionExpr
        '''
        super().__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        l = ExprNodeExec(self.ctx, self.stmt.lexpr).Execute()
        r = ExprNodeExec(self.ctx, self.stmt.rexpr).Execute()
        return l & r

class IntersectionExec(Executor):
    def __init__(self, ctx=None, stmt = None):
        '''
        @type stmt: IntersectionExpr
        '''
        super().__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        l = ExprNodeExec(self.ctx, self.stmt.lexpr).Execute()
        r = ExprNodeExec(self.ctx, self.stmt.rexpr).Execute()
        return l | r

class ExprNodeExec(Executor):
    def __init__(self, ctx=None, expr = None):
        '''
        @type Where: ExprNode
        '''
        super().__init__(ctx)
        self.expr = expr
        
    def Execute(self): 
        if isinstance(self.Where, ConditionExpr):
            e = ConditionExec(self.ctx, self.expr)
        elif isinstance(self.Where, UnionExpr):
            e = UnionExec(self.ctx, self.expr)
        else:
            e = IntersectionExec(self.ctx, self.expr)
        return e.Execute()
           

class SelectExec(Executor):
    def __init__(self, ctx=None):
        '''
        @type self.stmt: SelectStmt
        '''
        super().__init__(ctx)
       
        
    def Execute(self):
        if False:
            self.stmt = SelectStmt()
        self.stmt = SelectStmt() 
        BeginExec(self.ctx).Execute()
        
        rowids = ExprNodeExec(self.ctx, self.stmt.Where).Execute()
        
        
        rs  = list()
        if len(rowids) == 0:
            pass
        elif len(rowids) == 1:
            for field in self.stmt.Fields:
                f = PointGetExec(self.ctx, field).Execute()
                rs.append(f)
        else:
            for field in self.stmt.Fields:
                f = BatchGetExec(self.ctx, field).Execute()
                rs.append(f)
        
        CommitExec(self.ctx).Execute()
        return rs
        
        