import unittest
import os
from mylog import logger

import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
from union_store import *


nil = None

class ColumnStoreTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ColumnStoreTestCase, self).__init__(*args, **kwargs)
        
    @classmethod
    def setUpClass(cls):
        print('setUpClass')

    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    def setUp(self): 
        print'\n------',self._testMethodName, '-------'
        print "setUp..." 
        self.cs = ColumnStore(col = 0, txn_id = 0)

    def tearDown(self):
        print 'tearDown...'
        
    def mustGetNone(self, key):
        val = self.cs.Get(key)
        self.assertEqual(val, None)    
    
    def mustGetOK(self, key, expect):
        val  = self.cs.Get(key)
        self.assertEqual(val, expect)
        
    
    def mustSetOK(self, key, value):
        err = self.cs.Set(key, value)
        self.assertEqual(err, None)
    
    def mustDeleteOK(self, key):
        err = self.cs.Delete(key)
        self.assertEqual(err, None)
    
    def mustInsertOK(self, key, value):
        err = self.cs.Insert(key, value)
        self.assertEqual(err, None)
    
    def mustInsertErr(self, key, value):
        err = self.cs.Insert(key, value)
        self.assertNotEqual(err, None)
            
    def mustBatchGetOK(self, keys, expect):
        ret_dict = self.cs.BatchGet(keys)
        logger.debug(ret_dict)
        logger.debug(expect)
        self.assertEqual(ret_dict, expect)
        
    def mustScanOK(self, start, end, expect):
        pairs = self.cs.Scan(start, end)
        logger.debug(pairs)
        logger.debug(expect)
        self.assertEqual(len(pairs), len(expect))
        for i in range(len(pairs)):
            self.assertEqual(pairs[i][0], expect[i][0])
            self.assertEqual(pairs[i][1], expect[i][1])
    
    def mustLockkeyOK(self, key):
        err = self.cs.Lockkey(key)
        self.assertEqual(err, None)
        
    def mustLockkeysOK(self, keys):
        err = self.cs.Lockkeys(keys)
        self.assertEqual(err, None)
        
        
    def mustWalkBufferOK(self, expect):
        self.cs.WalkBuffer()
        mutations = self.cs.Mutations()
        logger.debug(mutations)
        logger.debug(expect)
        self.assertEqual(len(mutations), len(expect))
        for i in range(len(mutations)):
            self.assertEqual(mutations[i], expect[i])
    
    def test_Get(self):
        self.mustGetNone("k1")
        self.mustSetOK('k1', 'v1')
        self.mustGetOK('k1', 'v1')
        self.mustSetOK('k1', 'v2')
        self.mustGetOK('k1', 'v2')
        
    def test_Delete(self):
        self.mustGetNone("k1")
        self.mustSetOK('k1', 'v1')
        self.mustGetOK('k1', 'v1')
        self.mustDeleteOK('k1')
        self.mustGetNone('k1')

    def test_Set(self):
        self.mustGetNone("k1")
        self.mustSetOK('k1', 'v1')
        self.mustGetOK('k1', 'v1')
        self.mustSetOK('k2', 'v2')
        self.mustGetOK('k2', 'v2')
        self.mustSetOK('k1', None)
        self.mustGetNone('k1')
        
    def test_Insert(self):
        self.mustInsertOK('k1', 'v1')
        self.mustGetOK('k1', 'v1')
        self.mustInsertOK('k2', 'v2')
        self.mustGetOK('k2', 'v2')
        self.mustInsertErr('k1', 'v3')
                  
    def test_BatchGet(self):
        self.mustSetOK("k1", "v1")
        self.mustSetOK("k2", "v2")
        self.mustSetOK("k3", "v3")
        keys = ["k1", "k2", "k3","k4"]
        expect = {
            'k1':'v1',
            'k2':'v2',
            'k3':'v3',
            }
        self.mustBatchGetOK(keys, expect)
    
     
    def test_Scan(self):
        # ver10: A(10) - B(_) - C(10) - D(_) - E(10)
        self.mustSetOK("A", "A10")
        self.mustSetOK("C", "C10")
        self.mustSetOK("E", "E10")
     
        self.mustScanOK("", "A", [])
        self.mustScanOK("", "B", [('A','A10')])
        self.mustScanOK("", "C", [('A','A10')])
        self.mustScanOK("", "D", [('A','A10'),('C','C10')])
        self.mustScanOK("", "E", [('A','A10'),('C','C10')])
        self.mustScanOK("", "F", [('A','A10'),('C','C10'),('E','E10')])
        self.mustScanOK("", "", [('A','A10'),('C','C10'),('E','E10')])
    
    def test_LockKey(self):
        self.mustGetNone('k1')
        self.mustLockkeyOK('k1')
    
    def test_LockKeys(self):
        self.mustGetNone('k1')
        self.mustLockkeysOK(['k1','k2'])
    
    
    def test_WalkBuffer(self):
        def setV10():
            self.mustSetOK('A', 'A10')
            self.mustSetOK('C', None)
            self.mustInsertOK('E', 'E10')
        setV10()
        mutations = [
            kvrpcpb.Mutation(op=kvrpcpb.Put,key='A',value='A10'),
            kvrpcpb.Mutation(op=kvrpcpb.Del,key='C',value=None),
            kvrpcpb.Mutation(op=kvrpcpb.Insert,key='E',value='E10'),
        ]
       
        self.mustWalkBufferOK(mutations)

        def deleteV10():
            self.mustDeleteOK('E')
         
        deleteV10()
        mutations = [
            kvrpcpb.Mutation(op=kvrpcpb.Put,key='A',value='A10'),
            kvrpcpb.Mutation(op=kvrpcpb.Del,key='C',value=None),
            kvrpcpb.Mutation(op=kvrpcpb.Del,key='E',value=None),
        ]
        self.mustWalkBufferOK(mutations)
        
        def lockV10():
            self.mustLockkeyOK('A')
            self.mustLockkeyOK('G')

        lockV10()
        mutations = [
            kvrpcpb.Mutation(op=kvrpcpb.Put,key='A',value='A10'),
            kvrpcpb.Mutation(op=kvrpcpb.Del,key='C',value=None),
            kvrpcpb.Mutation(op=kvrpcpb.Del,key='E',value=None),
            kvrpcpb.Mutation(op=kvrpcpb.Lock,key='G',value=None),
        ]
        self.mustWalkBufferOK(mutations)
        
        logger.debug('primary key is: %s' % self.cs.primary)
        
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        ColumnStoreTestCase('test_Get'), 
        ColumnStoreTestCase('test_Delete'),
        ColumnStoreTestCase('test_Set'),
        ColumnStoreTestCase('test_Insert'),
        ColumnStoreTestCase('test_BatchGet'),
        ColumnStoreTestCase('test_Scan'),
        ColumnStoreTestCase('test_LockKey'),
        ColumnStoreTestCase('test_LockKeys'),
        ColumnStoreTestCase('test_WalkBuffer'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)
#     
     
#     unittest.main()
