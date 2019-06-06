import time
from util.codec import number

class Status():

    def __init__(self, err=None, autoCommit=True,
                 foundRows=0, affectedRows=0, lastInsertId=0):
        '''
        @param err: util.error.KvError
        '''
        self.err = err
        self.start_time = time.time()

        self.autoCommit = autoCommit
        self.foundRows = foundRows  # for select
        self.affectedRows = affectedRows  # for update/delete/insert
        self.lastInsertId = lastInsertId  # for insert
    
    def Info(self):
        info = dict()
        info['autoCommit'] = self.autoCommit
        info['time'] = time.time() - self.start_time
        
        if self.err:
            info['code'] = self.err.code
            info['msg'] = self.err.msg
            return info
        
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
        self.start_time = time.time()
        self.sql = sql  # set by server
        self.conn = conn  # set by conn
        self.session = session  # set by session
        self.stmt = stmt  # set by parser
        self.executor = executor  # set by plan
        self.status = Status()  # set by executor
        self.fields = None  # set by executor
        self.rows = None  # set by executor
        self.fieldTypes = None
        
    def Store(self):
        return self.session.store
    
    def Txn(self):
        return self.session.txn
        
    def SetErr(self, err):
        '''
        @param err: util.error.KvError
        '''
        self.status.err = err
    
    def TimeUsed(self):
        end_time = time.time()
        return end_time - self.start_time
    
def WriteDataSets(fields, rows, usetime):
    fi = ':>20d'
    fs = ':<20s'
    if not (rows and len(rows)):
        return None
    
    info = ''
    
    sepline = '+'
    for _ in xrange(len(fields)):
        sepline += '{%s}+' % fs
    sepline = sepline.format(*(['-'*20]*len(fields)))
#     print sepline
    info += '%s\n' % sepline
    
    title = '|'
    for _ in xrange(len(fields)):
        title += '{%s}|' % fs
    title = title.format(*fields)
#     print title
    info += '%s\n' % title
    info += '%s\n' % sepline
    
    frow = '|'
    for i in xrange(len(fields)):
        if isinstance(rows[0][i] , str):
            f = fs
        else:
            f = fi
        frow += '{%s}|' % f
#     print frow
    for row in rows:
        r = frow.format(*row)
        info += '%s\n' % r
    info += '%s\n' % sepline
    info += '%d rows in set (%.2f sec)' % (len(rows), usetime)
    return info

def WriteErr(err):
    '''
    @param err: util.error.KvError
    '''
    info = "ERROR %d: %s" % (err.code, err.msg)
    return info

def WriteOk(affectedRows, usetime):
    info = "Query OK, %d row affected (%.2f sec)" % (affectedRows, usetime)
    return info
        
if __name__ == '__main__':
    fields = ['id', 'name', 'age']
    rows = [
        [0L, 'n0', 5L], 
        [1L, 'n1', 5L],
        ]             
    print WriteDataSets(fields, rows) 
