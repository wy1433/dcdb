from sql.server.context import Context
from sql.parser.statement import Statement, ResultField


class Executor(object):

    def __init__(self, ctx=None):
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
        err = self.ctx.session.BeginTxn(self.ctx)
        if err is None:
            self.ctx.session.SetAutoCommit(False)
        return err

    
class CommitExec(Executor):

    def __init__(self, ctx=None):
        super(CommitExec, self).__init__(ctx)
        
    def Execute(self):
        err = self.ctx.session.CommitTxn(self.ctx)
        if err is None:
            self.ctx.session.SetAutoCommit(True)
        return err

    
class RollBackExec(Executor):

    def __init__(self, ctx=None):
        super(RollBackExec, self).__init__(ctx)
        
    def Execute(self):
        err = self.ctx.session.RollbackTxn(self.ctx)
        if err is None:
            self.ctx.session.SetAutoCommit(True)
        return err

