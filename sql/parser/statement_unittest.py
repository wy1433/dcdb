import unittest
from mylog import logger

from statement import *

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
        self.assertEqual(s.Setlist, [['1', 'foo', '10']])
        
        sql = "insert into student (id, name, age) values (1, 'foo', 10), (2, 'bob', 12), (3, 'tom', 9);"
        s = InsertStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        self.assertEqual(s.Table, 'student')
        self.assertEqual(s.Fields, ['id', 'name', 'age'])
        self.assertEqual(s.Setlist, [['1', 'foo', '10'],  ['2', 'bob', '12'], ['3', 'tom', '9']])
        
    def test_ConditionExpr(self):
        ''' Expression like this:
        c > start
        c < end
        c between start and end
        c = v
        c in (v1, v2, v3)
        '''
        sql = "age > 10"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start='10', end=None,
                               value=None, values=None, include_start=False, include_end=False)
        self.assertEqual(c.__dict__, expect.__dict__)
         
         
        sql = "age >= 10"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start='10', end=None,
                               value=None, values=None, include_start=True, include_end=False)
        self.assertEqual(c.__dict__, expect.__dict__)
        
        sql = "age < 10"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start=None, end='10',
                               value=None, values=None, include_start=False, include_end=False)
        self.assertEqual(c.__dict__, expect.__dict__)
         
        sql = "age <= 10"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start=None, end='10',
                               value=None, values=None, include_start=False, include_end=True)
        self.assertEqual(c.__dict__, expect.__dict__)
        
        
        sql = "age between 5 and 10"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start='5', end='10',
                               value=None, values=None, include_start=True, include_end=True)
        self.assertEqual(c.__dict__, expect.__dict__)
        
        
        sql = "age = 10"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start=None, end=None,
                               value='10', values=None, include_start=False, include_end=False)
        self.assertEqual(c.__dict__, expect.__dict__)
        
        sql = "age in (5, 10)"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start=None, end=None,
                               value=None, values=['5', '10'], include_start=False, include_end=False)
        self.assertEqual(c.__dict__, expect.__dict__)
        
        sql = "age in ('5', '10')"
        c = ConditionExpr(sql)
        r = c.Parse()
        self.assertIsNone(r)
        expect = ConditionExpr(text=sql, column='age', start=None, end=None,
                               value=None, values=['5', '10'], include_start=False, include_end=False)
        self.assertEqual(c.__dict__, expect.__dict__)
        
    def mustEqualExpr(self, e, t, rtext):
        self.assertIsInstance(e, t)
        self.assertEqual(e.rtext, rtext)
        return e.lexpr
    
    def mustEqualConditionExpr(self, e, text):
        self.assertIsInstance(e, ConditionExpr)
        self.assertEqual(e.text, text)
        
    
    def test_ExprNode(self):
        ''' Expression like this:
        c > start
        c < end
        c between start and end
        c = v
        c in (v1, v2, v3)
        '''
        sql = "age > 10 and age < 20 or id between 1 and 5 and name = 'bob' or age in (1, 3, 5)"
        e = ExprNode.GetExpr(sql)
        e = self.mustEqualExpr(e, UnionExpr, "age in (1, 3, 5)")
        e = self.mustEqualExpr(e, IntersectionExpr, "name = 'bob'")
        e = self.mustEqualExpr(e, UnionExpr, "id between 1 and 5")
        e = self.mustEqualExpr(e, IntersectionExpr, "age < 20")
        e = self.mustEqualConditionExpr(e, "age > 10")
    
    def test_select(self):
        sql = '''SELECT a, b, c From t \
                WHERE a > 10 \
                AND   b < -10 \
                OR    d between -10 and 10 \
                OR    e in ("a","b","c") \
                AND   f = "foo"
                '''
        sql = PreParse(sql)
        s = SelectStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        self.assertEqual(s.Table, 't')
        self.assertEqual(s.Fields, ['a', 'b', 'c'])
        self.mustEqualExpr(s.Where, IntersectionExpr, "f = \"foo\"")
        
    def test_delete(self):
        sql = '''DELETE FROM table_name \
                WHERE a > 10 \
                AND   b < -10 \
                OR    d between -10 and 10 \
                OR    e in ("a","b","c") \
                AND   f = "foo"
                '''
        sql = PreParse(sql)
        s = DeleteStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        self.assertEqual(s.Table, 'table_name')
        self.mustEqualExpr(s.Where, IntersectionExpr, "f = \"foo\"")
    
    def test_update(self):
        sql = '''UPDATE student \
                SET id = 1, name='foo', age = 10 \
                WHERE a > 10 \
                AND   b < -10 \
                OR    d between -10 and 10 \
                OR    e in ("a","b","c") \
                AND   f = "foo"
                '''
    
        sql = PreParse(sql)
        s = UpdateStmt(sql)
        r = s.Parse()
        self.assertIsNone(r)
        
        self.assertEqual(s.Table, 'student')
        self.assertEqual(s.Fields, ['id', 'name', 'age'])
        self.assertEqual(s.Setlist, ['1', 'foo', '10'])
        self.mustEqualExpr(s.Where, IntersectionExpr, "f = \"foo\"")
     
          
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        StatementTestCase('test_begin'),
        StatementTestCase('test_commit'),
        StatementTestCase('test_rollback'),
        StatementTestCase('test_insert'),
        StatementTestCase('test_ConditionExpr'),
        StatementTestCase('test_ExprNode'),
        StatementTestCase('test_select'),
        StatementTestCase('test_delete'),
        StatementTestCase('test_update'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)

#     unittest.main()
