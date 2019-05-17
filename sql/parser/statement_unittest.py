import unittest
from mylog import logger

from statement import *
from Cython.Plex.Actions import Begin

nil = None

class StatementTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(StatementTestCase, self).__init__(*args, **kwargs)
        
    @classmethod
    def setUpClass(cls):
        print('setUpClass')
        
    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    def setUp(self): 
        print'\n------', self._testMethodName, '-------'
        print "setUp..." 
             
    def tearDown(self):
        print 'tearDown'
        
        
    
 
    def test_begin(self):
        sql = "begin"
        s = BeginStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        sql = "start"
        s = BeginStmt(sql)
        r = s.Parse()
        self.assertEqual(r, ErrInvalidSql)
    
    def test_commit(self):
        sql = "commit"
        s = CommitStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        sql = "end"
        s = CommitStmt(sql)
        r = s.Parse()
        self.assertEqual(r, ErrInvalidSql)
    
    def test_rollback(self):
        sql = "rollback"
        s = RollBackStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        sql = "roll"
        s = RollBackStmt(sql)
        r = s.Parse()
        self.assertEqual(r, ErrInvalidSql)
    
    def test_insert(self):
        sql = "insert into student (id, name, age) values (1, 'foo', 10)"
        s = InsertStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        self.assertEqual(s.Table, 'student')
        self.assertEqual(s.Fields, ['id', 'name', 'age'])
        self.assertEqual(s.Setlist, ['1', 'foo','10'])
    
    def test_delete(self):
        pass
    
    def test_update(self):
        pass
    
    def test_select(self):
        pass
        
          
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        StatementTestCase('test_begin'),
        StatementTestCase('test_commit'),
        StatementTestCase('test_rollback'),
        StatementTestCase('test_insert'),
#         StatementTestCase('test_delete'),
#         StatementTestCase('test_update'),
#         StatementTestCase('test_select'),
#         StatementTestCase('test_delete'),
#         StatementTestCase('test_insert'),
#         StatementTestCase('test_delete'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)

#     unittest.main()
