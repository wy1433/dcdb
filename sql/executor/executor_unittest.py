import unittest
import os
from mylog import logger
import conf
from sql.kv.dckv import DckvStore
from sql.session.session import Session
from meta.infoschema import TableInfo, ColumnInfo, FieldType, IndexType
from executor import *
from select import *
from insert import *
from delete import *
from update import *

nil = None

class ExecutorTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ExecutorTestCase, self).__init__(*args, **kwargs)
        
    @classmethod
    def setUpClass(cls):
        print('setUpClass')
        
    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    def setUp(self):  
        print'\n------', self._testMethodName, '-------'
        print "setUp..." 
        self.init_db()
        self.session = Session(0, self.store)
        
    def tearDown(self):
        self.store.Close()
        print 'tearDown...'
 
    def init_db(self):
        os.system('rm -rf %s/store/*' % conf.dataPath)
        os.system('rm -rf %s/meta/*' % conf.dataPath)
        Student = TableInfo(1, 'student', [
        ColumnInfo(0, 'row_id', FieldType.INT, IndexType.UNIQUE),
        ColumnInfo(1, 'id', FieldType.INT, IndexType.UNIQUE),
        ColumnInfo(2, 'name', FieldType.STR, IndexType.NORMAL),
        ColumnInfo(3, 'age', FieldType.INT, IndexType.NORMAL),
        ])
    
        Test = TableInfo(2, 'test', [
            ColumnInfo(0, 'row_id', FieldType.INT, IndexType.UNIQUE),
            ColumnInfo(1, 'id', FieldType.INT, IndexType.UNIQUE),
            ColumnInfo(2, 'course', FieldType.STR, IndexType.NORMAL),
            ColumnInfo(3, 'score', FieldType.INT, IndexType.NORMAL),
            ColumnInfo(4, 'comment', FieldType.STR, IndexType.NORMAL),
            ])
        
        TABLES = [
            Student,
            Test,
            ]
        self.store = DckvStore(tables=TABLES)
    
    def init_context(self, sql):
        ctx = Context(sql=sql)
        ctx.store = self.store
        ctx.session = self.session
        return ctx
    
    def mustExecOk(self, sql):
        ctx = self.init_context(sql)
        err = ctx.session.Execute(ctx)
        self.assertIsNone(err)
        return ctx
        
    def mustExecErr(self, sql, e):
        ctx = self.init_context(sql)
        err = ctx.session.Execute(ctx)
        self.assertEqual(err, e)
        return ctx
        
    def test_begin(self):
        self.mustExecOk("begin")
        self.mustExecErr("begin", ErrTxnAlreadyExists)
    
    def test_commit(self):
        self.mustExecOk("begin")
        self.mustExecOk("commit")
        self.mustExecErr("commit", ErrInvalidTxn)
    
    def test_rollback(self):
        self.mustExecOk("begin")
        self.mustExecOk("rollback")
        self.mustExecErr("rollback", ErrInvalidTxn)
    
    def test_insert(self):
        self.mustExecOk("insert into student (id, name, age) values (1, 'foo1', 11)")
        self.mustExecOk("insert into student (id, name, age) values (2, 'foo2', 12)")
        self.mustExecErr("insert into student (id, name, age) values (2, 'foo3', 13)", 
                         ErrKeyExists)
        
        self.mustExecOk("insert into student (id, name, age) values (3, 'foo3', 13), (4, 'foo4', 14);")
        self.mustExecErr("insert into student (id, name, age) values (5, 'foo5', 15), (5, 'foo5', 15);",
                         ErrKeyExists)
    
    def mustGetCond(self, cond, ids):
        sql = "select id, name, age from student where %s" % cond
        ctx = self.init_context(sql)
        self.session.parser.Parser(ctx)
        self.assertIsInstance(ctx.stmt, SelectStmt)
        rowids, err = ExprNodeExec(ctx, ctx.stmt.Where).Execute()
        self.assertIsNone(err)
        expect = set([EncodeRowid(i) for i in ids])
        self.assertEqual(rowids, expect)
    
    def test_ConditionExec(self):
        self.mustExecOk("insert into student (id, name, age) values (-3, 'n3', 10)")
        self.mustExecOk("insert into student (id, name, age) values (0, 'n0', 5)")
        self.mustExecOk("insert into student (id, name, age) values (3, 'n3', 5)")
        
        # > , >=
        self.mustGetCond('id > -100', [1, 2, 3])
        self.mustGetCond('id >= -3', [1, 2, 3])
        self.mustGetCond('id > -3', [2, 3])
        self.mustGetCond('id >= 0', [2, 3])
        self.mustGetCond('id > 0', [3])
        self.mustGetCond('id >= 3', [3])
        self.mustGetCond('id > 3', [])
        self.mustGetCond('id > 100', [])
         
        # < , <=
        self.mustGetCond('id < -100', [])
        self.mustGetCond('id < -3', [])
        self.mustGetCond('id <= -3', [1])
        self.mustGetCond('id < 0', [1])
        self.mustGetCond('id <= 0', [1, 2])
        self.mustGetCond('id < 3', [1, 2])
        self.mustGetCond('id <= 3', [1, 2, 3])
        self.mustGetCond('id < 100', [1, 2, 3])
         
        # between and
        self.mustGetCond('id between -100 and 100', [1, 2, 3])
        self.mustGetCond('id between -3 and 3', [1, 2, 3])
         
        self.mustGetCond('id between 0 and 3', [2, 3])
        self.mustGetCond('id between 3 and 3', [3])
        self.mustGetCond('id between 100 and 3', [])
        
        self.mustGetCond('id between -3 and 0', [1, 2])
        self.mustGetCond('id between -3 and -3', [1])
        self.mustGetCond('id between -3 and -100', [])
         
        self.mustGetCond('name between "n2" and "n3"', [1, 3])  
         
        # =
        self.mustGetCond('id = -3', [1])
        self.mustGetCond('id = 0', [2])
        self.mustGetCond('id = 1', [])
        self.mustGetCond('name = n3', [1, 3])
        self.mustGetCond('age = 5', [2, 3])
         
        # in
        self.mustGetCond('id in (-3, 0, 3)', [1, 2, 3])
        self.mustGetCond('id in (-1, 0, 1)', [2])
        self.mustGetCond('age in (5, 10)', [1, 2, 3])
        self.mustGetCond('name in ("n0", "n1", "n3" )', [1, 2, 3])
    
    def test_UnionExec(self):
        self.mustExecOk("insert into student (id, name, age) values (-3, 'n3', 10)")
        self.mustExecOk("insert into student (id, name, age) values (-1, 'n1', 10)")
        self.mustExecOk("insert into student (id, name, age) values (0, 'n0', 5)")
        self.mustExecOk("insert into student (id, name, age) values (1, 'n1', 5)")
        self.mustExecOk("insert into student (id, name, age) values (3, 'n3', 10)")
        
        
        self.mustGetCond('id > 0', [4, 5])
        self.mustGetCond('id < 0', [1, 2])
        self.mustGetCond('id > 0 or id < 0', [1, 2, 4, 5])
        
        self.mustGetCond('name = n1', [2, 4])
        self.mustGetCond('name = n0', [3])
        self.mustGetCond('name = n1 or name = n0', [2, 3, 4])
        
        self.mustGetCond('age in (5, 8)', [3, 4])
        self.mustGetCond('age between 10 and 20', [1, 2, 5])
        self.mustGetCond('age in (5, 8) or age between 10 and 20', [1, 2, 3, 4, 5])
        
        self.mustGetCond('id > 0 or name = n1 or age in (5, 8)', [2, 3, 4, 5])
    
    
    def test_IntersectionExec(self):
        self.mustExecOk("insert into student (id, name, age) values (-3, 'n3', 10)")
        self.mustExecOk("insert into student (id, name, age) values (-1, 'n1', 10)")
        self.mustExecOk("insert into student (id, name, age) values (0, 'n0', 5)")
        self.mustExecOk("insert into student (id, name, age) values (1, 'n1', 5)")
        self.mustExecOk("insert into student (id, name, age) values (3, 'n3', 10)")
        
        
        self.mustGetCond('id > -2', [2, 3, 4, 5])
        self.mustGetCond('id < 2', [1, 2, 3, 4])
        self.mustGetCond('id > -2 and id < 2', [2, 3, 4])
        
        self.mustGetCond('name = n1', [2, 4])
        self.mustGetCond('name < n2', [2, 3, 4])
        self.mustGetCond('name = n1 and name < n2', [2, 4])
        
        self.mustGetCond('age in (5, 8)', [3, 4])
        self.mustGetCond('age between 5 and 20', [1, 2, 3, 4, 5])
        self.mustGetCond('age in (5, 8) and age between 5 and 20', [3, 4])
        
        self.mustGetCond('id > -2 and name = n1 and age in (5, 8)', [4])
    
    def test_ExprNodeExec(self):
        self.mustExecOk("insert into student (id, name, age) values (-3, 'n3', 10)")
        self.mustExecOk("insert into student (id, name, age) values (-1, 'n1', 10)")
        self.mustExecOk("insert into student (id, name, age) values (0, 'n0', 5)")
        self.mustExecOk("insert into student (id, name, age) values (1, 'n1', 5)")
        self.mustExecOk("insert into student (id, name, age) values (3, 'n3', 10)")
        
        
        self.mustGetCond('id > -2', [2, 3, 4, 5])
        self.mustGetCond('id < 2', [1, 2, 3, 4])
        self.mustGetCond('name = n3', [1, 5])
        self.mustGetCond('age in (5, 8)', [3, 4])
       
        self.mustGetCond('id > -2 and id < 2 or name = n3 and age in (5, 8)', [3, 4])
      

    def test_select(self):
        self.mustExecOk("insert into student (id, name, age) values (-3, 'n3', 10);")
        self.mustExecOk("insert into student (id, name, age) values (-1, 'n1', 10);")
        self.mustExecOk("insert into student (id, name, age) values (0, 'n0', 5);")
        self.mustExecOk("insert into student (id, name, age) values (1, 'n1', 5);")
        self.mustExecOk("insert into student (id, name, age) values (3, 'n3', 10);")
        
        sql = '''SELECT id, name, age From student \
                WHERE id > -2 \
                AND   id < 2 \
                OR    name = 'n3' \
                AND    age in (5, 8)
                '''
        ctx = self.mustExecOk(sql)
        logger.debug(ctx.rows)
        self.assertEqual(ctx.fields, ['id', 'name', 'age'])
        self.assertEqual(ctx.rows, [
            [0, 'n0', 5],
            [1, 'n1', 5],
            ])
        
        
    def test_delete(self):
        self.mustExecOk("insert into student (id, name, age) values (-3, 'n3', 10);")
        self.mustExecOk("insert into student (id, name, age) values (-1, 'n1', 10);")
        self.mustExecOk("insert into student (id, name, age) values (0, 'n0', 5);")
        self.mustExecOk("insert into student (id, name, age) values (1, 'n1', 5);")
        self.mustExecOk("insert into student (id, name, age) values (3, 'n3', 10);")
        
        sql = '''DELETE FROM student \
                WHERE id > -2 \
                AND   id < 2 \
                OR    name = 'n3' \
                AND    age in (5, 8)
                '''
        # row 3, 4 will be deleted
        ctx = self.mustExecOk(sql)
        self.assertEqual(ctx.status.affectedRows, 2)
        
        
        sql = '''SELECT id, name, age From student where id > -100'''
        ctx = self.mustExecOk(sql)
        logger.debug(ctx.rows)
        self.assertEqual(ctx.fields, ['id', 'name', 'age'])
        self.assertEqual(ctx.rows, [
            [-3, 'n3', 10],
            [-1, 'n1', 10],
            [3, 'n3', 10],
            ])
        
    
    def test_update(self):
        self.mustExecOk("insert into student (id, name, age) values (-3, 'n3', 10);")
        self.mustExecOk("insert into student (id, name, age) values (-1, 'n1', 10);")
        self.mustExecOk("insert into student (id, name, age) values (0, 'n0', 5);")
        self.mustExecOk("insert into student (id, name, age) values (1, 'n1', 5);")
        self.mustExecOk("insert into student (id, name, age) values (3, 'n3', 10);")
    
    
        sql = '''UPDATE student \
                SET age = 6 \
                WHERE id > -2 \
                AND   id < 2 \
                OR    name = 'n3' \
                AND    age in (5, 8)
                '''
        
        # row 3, 4 will be updated
        ctx = self.mustExecOk(sql)
        self.assertEqual(ctx.status.affectedRows, 2)
     
     
        sql = '''SELECT id, name, age From student where id in (0, 1)'''
        ctx = self.mustExecOk(sql)
        logger.debug(ctx.rows)
        self.assertEqual(ctx.fields, ['id', 'name', 'age'])
        self.assertEqual(ctx.rows, [
            [0, 'n0', 6],
            [1, 'n1', 6],
            ])
          
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        ExecutorTestCase('test_begin'),
        ExecutorTestCase('test_commit'),
        ExecutorTestCase('test_rollback'),
        ExecutorTestCase('test_insert'),
        ExecutorTestCase('test_ConditionExec'),
        ExecutorTestCase('test_UnionExec'),
        ExecutorTestCase('test_IntersectionExec'),
        ExecutorTestCase('test_ExprNodeExec'),
        ExecutorTestCase('test_select'),
        ExecutorTestCase('test_delete'),
        ExecutorTestCase('test_update'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)

#     unittest.main()
