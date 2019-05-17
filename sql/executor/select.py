from sql.server.context import Context
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from util.codec.table import *


def HasIndex(col):
    return True


class PointGetExec(Executor):

    def __init__(self, ctx=None, col=None, key=None):
        '''
        @type ctx: Context
        '''
        super(PointGetExec, self).__init__(ctx)
        self.col = col
        self.key = key
        
    def Execute(self):
        ctx = self.ctx #: :type ctx: Context
        stmt = ctx.stmt #: :type stmt: SelectStmt
        store = ctx.Store()
        txn = ctx.Txn()
        c = store.meta.GetColumnInfoByName(stmt.Table, self.col)
        
        
        return self.ctx.txn.Get(self.key, self.col)


class BatchGetExec(Executor):

    def __init__(self, ctx=None, col=None, keys=None):
        super(BatchGetExec, self).__init__(ctx)
        self.col = col
        self.keys = keys

    def Execute(self):
        return self.ctx.txn.BatchGet(self.keys, self.col)

class ColumnScanExec(Executor):
    def __init__(self, ctx=None, col = None, start = None, end = None):
        super(ColumnScanExec, self).__init__(ctx)
    def Execute(self):
        '''
        @todo: implemented. scan + filter
        '''
        pass

class IndexScanExec(Executor):
    def __init__(self, ctx=None, col = None, start = None, end = None):
        '''
        @param ctx: Context
        @param col: str
        @param start: str
        @param end: end
        '''
        super(IndexScanExec, self).__init__(ctx)
        self.col = col
        self.start = start
        self.end = end
        
    def Execute(self):
        '''
        @rtype: set, ErrExecutor
        '''
        ctx = self.ctx #: :type ctx: Context
        stmt = ctx.stmt #: :type stmt: SelectStmt
        store = ctx.Store()
        txn = ctx.Txn()
        c = store.meta.GetColumnInfoByName(stmt.Table, self.col)
        idx = c.IndexDBName()
        startKey = EncodeIndexKey(None, self.start, c.fieldType, c.indexType)
        endKey = EncodeIndexKey(None, self.end, c.fieldType, c.indexType)
        ret, err = txn.Scan(startKey, endKey, limit=None, col=idx) #: :type ret: dict
        if err:
            logger.warning('err %s' , err.ERROR())
            return None, ErrExecutor
        r = set(ret.viewvalues())
        return r, None

class RangeScanExec(Executor):
    def __init__(self, ctx=None, col = None, start = None, end = None):
        super(RangeScanExec, self).__init__(ctx)
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
        super(ConditionExec, self).__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        if self.stmt.ConditionType == ConditionType.ConditionTypeEquals:
            return RangeScanExec(self.ctx, self.stmt.column, start= self.stmt.start, end=self.stmt.end).Execute()
        elif self.stmt.ConditionType == ConditionType.ConditionTypeIn:
            return RangeScanExec(self.ctx, self.stmt.column, start= self.stmt.start, end=self.stmt.end).Execute()
        elif self.stmt.ConditionType == ConditionType.ConditionTypeRangeScan:
            return RangeScanExec(self.ctx, self.stmt.column, start= self.stmt.start, end=self.stmt.end).Execute()
        else:
            return ErrExecutor

class UnionExec(Executor):
    def __init__(self, ctx=None, stmt = None):
        '''
        @type stmt: UnionExpr
        '''
        super(UnionExec, self).__init__(ctx)
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
        super(IntersectionExec, self).__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        l = ExprNodeExec(self.ctx, self.stmt.lexpr).Execute()
        r = ExprNodeExec(self.ctx, self.stmt.rexpr).Execute()
        return l | r

class ExprNodeExec(Executor):
    def __init__(self, ctx=None, expr = None):
        '''
        @param expr: ExprNode
        '''
        super(ExprNodeExec, self).__init__(ctx)
        self.expr = expr
        
    def Execute(self): 
        if isinstance(self.expr, ConditionExpr):
            e = ConditionExec(self.ctx, self.expr)
        elif isinstance(self.expr, UnionExpr):
            e = UnionExec(self.ctx, self.expr)
        else:
            e = IntersectionExec(self.ctx, self.expr)
        return e.Execute()
           

class SelectExec(Executor):
    def __init__(self, ctx=None):
        '''
        @param ctx: Context
        '''
        super(SelectExec, self).__init__(ctx)
       
        
    def Execute(self):        
        ctx = self.ctx #: :type ctx: Context
        stmt = ctx.stmt #: :type stmt: SelectStmt
        store = ctx.Store()
        txn = ctx.Txn()
        
        rowids = ExprNodeExec(ctx, stmt.Where).Execute()
        
        
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
        
        