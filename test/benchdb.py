import getopt
import os
import sys
import random
import threading
import time
from mylog import logger
from sql.server.server import Server, Context
from sql.kv.dckv import DckvStore
import conf
from meta.infoschema import TableInfo, ColumnInfo, FieldType, IndexType
from sql.session.session import Session

threads = 4
time = 600
interval = 10
table_size = 1000
data_size = 100

class BenchDB():
    def __init__(self):
        self.init_db()
#         self.init_data()
        
    def init_db(self):
        Student = TableInfo(1, 'student', [
        ColumnInfo(0, 'row_id', FieldType.INT, IndexType.UNIQUE),
        ColumnInfo(1, 'id', FieldType.INT, IndexType.UNIQUE),
        ColumnInfo(2, 'name', FieldType.STR, IndexType.NORMAL),
        ColumnInfo(3, 'age', FieldType.INT, IndexType.NORMAL),
        ])
        TABLES = [
            Student,
            ]
        self.store = DckvStore(tables=TABLES)
        
    def delete_db(self):
        os.system('rm -rf %s/store/*' % conf.dataPath)
        os.system('rm -rf %s/meta/*' % conf.dataPath)
    
    def init_data(self):
#         session = Session(0, self.store)
#         ctx = Context()
#         ctx.store = self.store
#         ctx.session = session
        base_sql = "insert into student (id, name, age) values (%d, '%s', %d)"
        for i in range(table_size):
            sid = i
            name = 'name%d' % sid
            age = random.uniform(6, 60)
            sql = base_sql % (sid, name, age)
            self.mustExecOk(sql)
    
    def Close(self):
        self.store.Close()
    
    def mustExecOk(self, sql):
        session = Session(0, self.store)
        ctx = Context(sql=sql)
        ctx.store = self.store
        ctx.session = session
        err = ctx.session.Execute(ctx)
        assert err is None
#         logger.info(ctx.status.Info())
        return ctx
    
    def Exec(self, sql):
        session = Session(0, self.store)
        ctx = Context(sql=sql)
        ctx.store = self.store
        ctx.session = session
        err = ctx.session.Execute(ctx)
        return ctx, err
    
    def test_insert(self):
        self.delete_db()
        s = Stat("insert")
        base_sql = "insert into student (id, name, age) values (%d, '%s', %d)"
        for i in range(table_size):
            sid = i
            name = 'name%d' % sid
            age = random.uniform(6, 60)
            sql = base_sql % (sid, name, age)
            ctx = self.mustExecOk(sql)
            s.GetReqTime(ctx.TimeUsed()*1000)
        s.StatInfo()
        
    def test_select(self):
        s = Stat("select")
        base_sql = "select id, name, age from student where id = %d"
        for i in range(table_size):
            sid = i
            sql = base_sql % (sid)
            ctx = self.mustExecOk(sql)
#             logger.info(ctx.rows)
            s.GetReqTime(ctx.TimeUsed()*1000)
        s.StatInfo()
        
    def test_update(self):
        s = Stat("update")
        base_sql = "update student set age = %d  where id = %d"
        for i in range(table_size):
            sid = i
            age = random.uniform(6, 60)
            sql = base_sql % (sid, age)
            ctx = self.mustExecOk(sql)
            s.GetReqTime(ctx.TimeUsed()*1000)
        s.StatInfo()
        
    def test_delete(self):
        s = Stat("delete")
        base_sql = "delete from student where id = %d"
        for i in range(table_size):
            sid = i
            sql = base_sql % (sid)
            ctx = self.mustExecOk(sql)
            s.GetReqTime(ctx.TimeUsed()*1000)
        s.StatInfo() 
    
class Stat():
    def __init__(self, name):
        self.name = name
        self.count = 0
        self.sum = 0
        self.max = 0
        self.min = sys.maxint
#         logger.info("----------- Start %s test -----------", self.name)
    
    def GetReqTime(self, t):
        self.count += 1
        self.sum += t
        self.max = max(self.max, t)
        self.min = min(self.min, t)
#         if self.count % 100 == 0:
#             logger.info("testing %d case ...", self.count)
    
    def StatInfo(self):
        info = 'test(ms)=%s, count=%d, avg=%.2f, max=%.2f, min=%.2f' % (
                self.name, self.count, self.sum/self.count, self.max, self.min)
        print info
#         logger.info('%s', info)
#         logger.info('\n%s\n%s\n%s', '*'*60, info, '*'*60)
#         logger.info("----------- End %s test -----------", self.name)
           
if __name__ == '__main__':
    name = 'insert'
    opts, args = getopt.getopt(sys.argv[1:],"s:",["table_size="])
    for opt, arg in opts:
        if opt in ("-s", "--table_size"):
            table_size = int(arg)
           
    db = BenchDB()
    db.test_insert()
    db.test_select()
    db.test_update()
    db.test_delete()
#     if name == 'insert':
#         db.test_insert()
#     else:
#         pass