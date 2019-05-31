import os
import random
# import time
import conf
from test.benchdb import BenchDB


class Transaction(object):
    def __init__(self):
        self.db = BenchDB()
        self.db.delete_db()
        self.base_sql = "insert into student (id, name, age) values (%d, '%s', %d)"
        self.id = 0

    def run(self):
        self.id += 1
        sid = self.id
        base_sql = self.base_sql
        
        name = 'name%d' % sid
        age = random.uniform(6, 60)
        sql = base_sql % (sid, name, age)
        ctx, err = self.db.Exec(sql)
        assert err is None, 'db exec with a err'
        self.usetime = ctx.TimeUsed()*1000

if __name__ == '__main__':
    trans = Transaction()
    trans.run()
    print trans.usetime
