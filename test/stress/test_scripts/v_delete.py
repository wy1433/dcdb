import os
import random
# import time
import conf
from test.benchdb import BenchDB


class Transaction(object):
    def __init__(self):
        self.db = BenchDB()
        self.base_sql = "delete from student where id = %d"
        self.id = 0

    def run(self):
        self.id += 1
        sid = self.id
        base_sql = self.base_sql
        
        sql = base_sql % (sid)
        ctx, err = self.db.Exec(sql)
        assert err is None, 'db exec with a err'
        self.usetime = ctx.TimeUsed()*1000

if __name__ == '__main__':
    trans = Transaction()
    trans.run()
    print trans.usetime
