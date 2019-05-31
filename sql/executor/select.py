from sql.server.context import Context
from sql.parser.statement import *
from sql.executor.executor import *
from util.error import *
from util.codec.table import *

MinRowID = 0
MaxRowID = 0x0FFFFFFFFFFFFFFF


class PointGetExec(Executor):

    def __init__(self, ctx=None, col=None, key=None):
        '''
        @type ctx: Context
        '''
        super(PointGetExec, self).__init__(ctx)
        self.col = col
        self.key = key
        
    def Execute(self):
        '''
        @rtype: str, ErrExecutor
        '''
        ctx = self.ctx  # : :type ctx: Context
        store = ctx.Store()
        txn = ctx.Txn()
        c = store.meta.GetColumnInfoByName(ctx.stmt.Table, self.col)
        dat = c.DataDBName()
        v, err = txn.Get(self.key, dat)
        if err:
            logger.warning('err %s' , err.ERROR())
            return None, ErrExecutor
        return v, None


class BatchGetExec(Executor):

    def __init__(self, ctx=None, col=None, keys=None):
        super(BatchGetExec, self).__init__(ctx)
        self.col = col
        self.keys = keys

    def Execute(self):
        '''
        @rtype: dict(str, str), ErrExecutor
        '''
        ctx = self.ctx  # : :type ctx: Context
        store = ctx.Store()
        txn = ctx.Txn()
        c = store.meta.GetColumnInfoByName(ctx.stmt.Table, self.col)
        dat = c.DataDBName()
        pairs, err = txn.BatchGet(self.keys, dat)
        if err:
            logger.warning('err %s' , err.ERROR())
            return None, ErrExecutor
        return pairs, None


class IndexScanExec(Executor):

    def __init__(self, ctx=None, col=None, start=None, end=None):
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
        @rtype: set(str), ErrExecutor
        '''
        ctx = self.ctx  # : :type ctx: Context
        store = ctx.Store()
        txn = ctx.Txn()
        c = store.meta.GetColumnInfoByName(ctx.stmt.Table, self.col)
        idx = c.IndexDBName()
        ret, err = txn.Scan(self.start, self.end, limit=None, col=idx)  # : :type ret: dict
        if err:
            logger.warning('err %s' , err.ERROR())
            return None, ErrExecutor
        r = set(ret.viewvalues())
        return r, None



class ConditionExec(Executor):

    def __init__(self, ctx=None, stmt=None):
        '''
        @param stmt: ConditionExpr
        @rtype: set(str), err
        '''
        super(ConditionExec, self).__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        '''
        @rtype: set(str), ErrExecutor
        '''
        c = self.ctx.store.meta.GetColumnInfoByName(self.ctx.stmt.Table, self.stmt.column)
        start = None
        end = None
        
        # for op: in
        if self.stmt.values:
            rowids = set()
            for value in self.stmt.values:
                start = EncodeIndexKey(MinRowID, value, c.fieldType, c.indexType)
                end = EncodeIndexKey(MaxRowID, value, c.fieldType, c.indexType)
                ids, err = IndexScanExec(self.ctx, self.stmt.column, start, end).Execute()
                if err:
                    return None, err
                rowids |= ids
            return rowids, None
        
        # for op:  > , >=
        if self.stmt.start:
            rowid = MinRowID if self.stmt.include_start else MaxRowID
            start = EncodeIndexKey(rowid, self.stmt.start, c.fieldType, c.indexType)
        
        # for op:  < , <=
        if self.stmt.end:
            rowid = MaxRowID if self.stmt.include_end else MinRowID
            end = EncodeIndexKey(rowid, self.stmt.end, c.fieldType, c.indexType)
        
        # for op: =
        if self.stmt.value:
            start = EncodeIndexKey(MinRowID, self.stmt.value, c.fieldType, c.indexType)
            end = EncodeIndexKey(MaxRowID, self.stmt.value, c.fieldType, c.indexType)
        
        rowids, err = IndexScanExec(self.ctx, self.stmt.column, start, end).Execute()
        if err:
            return None, err
        return rowids, err
        

class UnionExec(Executor):

    def __init__(self, ctx=None, stmt=None):
        '''
        @type stmt: UnionExpr
        '''
        super(UnionExec, self).__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        '''
        @rtype: set(str), ErrExecutor
        '''
        l, err = ExprNodeExec(self.ctx, self.stmt.lexpr).Execute()
        if err:
            return None, err
        r, err = ExprNodeExec(self.ctx, self.stmt.rexpr).Execute()
        if err:
            return None, err
        return l | r, None


class IntersectionExec(Executor):

    def __init__(self, ctx=None, stmt=None):
        '''
        @type stmt: IntersectionExpr
        '''
        super(IntersectionExec, self).__init__(ctx)
        self.stmt = stmt
        
    def Execute(self):
        '''
        @rtype: set(str), ErrExecutor
        '''
        l, err = ExprNodeExec(self.ctx, self.stmt.lexpr).Execute()
        if err:
            return None, err
        r, err = ExprNodeExec(self.ctx, self.stmt.rexpr).Execute()
        if err:
            return None, err
        return l & r, None

class ExprNodeExec(Executor):

    def __init__(self, ctx=None, expr=None):
        '''
        @param expr: ExprNode
        '''
        super(ExprNodeExec, self).__init__(ctx)
        self.expr = expr
        
    def Execute(self):
        '''
        @rtype: set(str), ErrExecutor
        '''
        if isinstance(self.expr, ConditionExpr):
            e = ConditionExec(self.ctx, self.expr)
        elif isinstance(self.expr, UnionExpr):
            e = UnionExec(self.ctx, self.expr)
        else:
            e = IntersectionExec(self.ctx, self.expr)
        rids, err = e.Execute()
        return rids, err
           

class SelectExec(Executor):

    def __init__(self, ctx=None):
        '''
        @param ctx: Context
        '''
        super(SelectExec, self).__init__(ctx)
        
    def Execute(self):        
        ctx = self.ctx  # : :type ctx: Context
        stmt = ctx.stmt  # : :type stmt: SelectStmt
        store = ctx.Store()
        
        # 1. get rowids from where
        rowids, err = ExprNodeExec(ctx, stmt.Where).Execute() #: :type rowids: set(str)
        if err:
            return err
        
        
        # 2. set to sorted list 
        rids = list(rowids)
        rids.sort()
        ctx.status.foundRows += len(rowids)
        
        # 3. get field by rids
        fields = stmt.Fields
        rows = list()
        
        if len(rowids) == 0:
            pass
        elif len(rids) == 1:
            row = list()
            for field in stmt.Fields:
                s, err = PointGetExec(self.ctx, field, rids[0]).Execute()
                c = store.meta.GetColumnInfoByName(stmt.Table, field)
                if err:
                    return ErrExecutor
                v = DecodeValue(s, c.fieldType) if s else None
                row.append(v)
            rows.append(row)   
        else:
            field_dict = dict()
            field_type = dict()
            for field in stmt.Fields:
                d, err = BatchGetExec(self.ctx, field, rids).Execute() #: :type d: dict(str, str)
                c = store.meta.GetColumnInfoByName(stmt.Table, field)
                if err:
                    return ErrExecutor
                field_dict[field] = d
                field_type[field] = c
            for rid in rids:
                row = list()
                for field in stmt.Fields:
                    if rid in field_dict[field]:
                        s = field_dict[field][rid]
                        v = DecodeValue(s, field_type[field].fieldType)
                    else:
                        v = None
                    row.append(v)
                rows.append(row)
        
        ctx.fields = fields
        ctx.rows = rows
        
