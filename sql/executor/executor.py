from sql.server.context import Context
from sql.parser.statement import Statement, ResultField


class Executor(object):

    def __init__(self, ctx = None):
        '''
        @param ctx: Context
        '''
        self.ctx = ctx
        self.children = list()  # type list[Executor]
        self.retFieldTypes = list()  # type list[FieldType]
        self.ResultSet = list()  # type: list[ResultField]
        
    def Execute(self):
        pass
    

class BeginExec(Executor):

    def __init__(self, ctx=None):
        super(BeginExec, self).__init__(ctx)
        
    def Execute(self):
        self.ctx.session.SetAutoCommit(False)
        _, err = self.ctx.session.Txn(self.ctx)
        return err

    
class CommitExec(Executor):

    def __init__(self, ctx=None):
        super(CommitExec, self).__init__(ctx)
        
    def Execute(self):
        return self.ctx.session.CommitTxn(self.ctx)

    
class RollBackExec(Executor):

    def __init__(self, ctx=None):
        super(RollBackExec, self).__init__(ctx)
        
    def Execute(self):
        return self.ctx.session.RollbackTxn(self.ctx)



    
    
