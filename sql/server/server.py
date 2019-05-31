from util.counter import Counter
from sql.server.conn import ClientConn
from sql.kv.dckv import DckvStore
from sql.session.session import Session, SessionPool
from mylog import logger
from sql.server.context import Context, Status

SessionCounter = Counter()
ConnectCounter = Counter()
RowCounter = Counter()

class Server():
    def __init__(self):
        self.store = DckvStore()
        self.session_pool = SessionPool()        
    
    def Run(self, sql, session_id=None):
        ''' Execute client's command and return response.
        command is a sql string request by http post request.
        sql is one of those kinds below:
            begin : start a transaction
            commit:  commits the current transaction, making its changes permanent.
            rollback : rolls back the current transaction, canceling its changes.
            delete:   delete rows by where expression
            insert:   insert one row
            update:   update rows by where expression
            select:   select rows by where expression
        @see: https://dev.mysql.com/doc/refman/8.0/en/commit.html
        @param sql: sql string
        @param session_id: session's id of the user client.
        '''
        ctx = Context(sql=sql)
        
        # 1. init store
        ctx.store = self.store
                
        # 2. init session
        err = None
        if session_id is None:
            session_id, err = self.login()
        if err:
            ctx.SetErr(err)
            ret = self.WriteResult(ctx)
            return  ret
        session = self.session_pool.Get(session_id)
#         if session is None:
#             ctx.SetErr(ErrInvalidSessionID)
#             ret = self.WriteResult(ctx)
#             return ret
        ctx.session = session
        
        # 3. init conn and execute sql
        conn_id = ConnectCounter.Incr()
        client_conn = ClientConn(conn_id)
        ctx.conn = client_conn
        client_conn.Run(ctx)
        
        # 4. write result to response
        ret = self.WriteResult(ctx)
        return ret
    
    def login(self):
        '''
        @rtype:  int, ErrSessionMaxSize
        '''
        session_id = SessionCounter.Incr()
        session = Session(session_id, self.store)
        err = self.session_pool.Set(session)
        logger.debug("sid=%d, err=%s", session_id, err)
        return session_id, err
    
    def logout(self, session_id):
        logger.debug("sid=%d", session_id)
        self.session_pool.Delete(session_id)
    
    def WriteResult(self, ctx):
        '''
        @param ctx: Context
        @return: dict
        '''
        resp = dict()
        if ctx.status:
            resp['status'] = ctx.status.Info()
        if ctx.fields:
            resp['fields'] = ctx.fields
        if ctx.rows:
            resp['rows'] = ctx.rows
        return resp
        
if __name__ == '__main__':
    server = Server()
    
#     sql = "insert into student (id, name, age) values (1, 'foo', 10)"
#     resp =  server.Run(sql)
#     logger.debug(resp)
#     sql = "insert into student (id, name, age) values (2, 'bob', 8)"
#     resp =  server.Run(sql)
#     logger.debug(resp)
    
    sql = "select id, name, age from student where id in (1, 2)"
    resp =  server.Run(sql)
    logger.debug(resp)
    
        