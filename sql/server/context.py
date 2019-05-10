class Status():

    def __init__(self, err=None, inTrans=False, autoCommit=True,
                 foundRows=None, affectedRows=None, lastInsertId=0):
        '''
        @param err: util.error.KvError
        '''
        self.err = err
        self.inTrans = inTrans
        self.autoCommit = autoCommit
        self.foundRows = foundRows  # for select
        self.affectedRows = affectedRows  # for update/delete/insert
        self.lastInsertId = lastInsertId  # for insert
    
    def Info(self):
        info = dict()
        if self.err:
            info['code'] = self.err.code
            info['msg'] = self.err.msg
        info['inTrans'] = self.inTrans
        info['autoCommit'] = self.autoCommit
        if self.foundRows:
            info['foundRows'] = self.foundRows
        if self.affectedRows:
            info['affectedRows'] = self.affectedRows
        if self.lastInsertId:
            info['lastInsertId'] = self.lastInsertId
        return info

        
class Context():

    def __init__(self, sql=None, conn=None, session=None, stmt=None, executor=None):
        '''
        @param sql: str
        @param conn: ClientConn
        @param session: sql.session.session.Session
        @param stmt: sql.parser.statement.Statement
        @param executor: sql.executor.executor.Executor
        @param status: Status
        '''
        self.sql = sql  # set by server
        self.conn = conn  # set by conn
        self.session = session  # set by session
        self.stmt = stmt  # set by parser
        self.executor = executor  # set by plan
        self.status = Status()  # set by executor
        self.fields = None  # set by executor
        self.rows = None  # set by executor
        
    def Store(self):
        return self.session.store
    
    def Txn(self):
        return self.session.txn
        
    def SetErr(self, err):
        '''
        @param err: util.error.KvError
        '''
        self.status.err = err
            
