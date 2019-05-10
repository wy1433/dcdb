import unittest
import os
import sys
from mylog import logger

import conf
import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
from txn import *
from dckv import *
from sql.kv.dckv import DckvStore
from meta.infoschema import TableInfo, ColumnInfo, FieldType, IndexType

from lock_resolver import *

nil = None

startIndex = 0
testCount  = 2
indexStep  = 2
col = 'student.name.data'

class DckvTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(DckvTestCase, self).__init__(*args, **kwargs)
        
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
        
    def tearDown(self):
        self.store.Close()
        os.system('rm -rf %s/store/*' % conf.dataPath)
        print 'tearDown', 'delete db ...'

    def init_db(self):
        Student = TableInfo(1, 'student', [
        ColumnInfo(0, 'row_id', FieldType.INT, IndexType.UNIQUE),
        ColumnInfo(1, 'id', FieldType.INT, IndexType.UNIQUE),
        ColumnInfo(2, 'name', FieldType.STR, IndexType.NORMAL),
        ColumnInfo(3, 'age', FieldType.INT, IndexType.NORMAL),
        ])
    
        test_ = TableInfo(2, 'test', [
            ColumnInfo(0, 'row_id', FieldType.INT, IndexType.UNIQUE),
            ColumnInfo(1, 'id', FieldType.INT, IndexType.UNIQUE),
            ColumnInfo(2, 'course', FieldType.STR, IndexType.NORMAL),
            ColumnInfo(3, 'score', FieldType.INT, IndexType.NORMAL),
            ColumnInfo(4, 'comment', FieldType.STR, IndexType.NORMAL),
            ])
        
        TABLES = [
            Student,
            test_,
            ]
        self.store = DckvStore(tables=TABLES)
    
    def mustSetOk(self, key, value, col, txn):
        '''
        @param txn: Transaction
        '''
        err = txn.Set(key, value, col)
        self.assertEqual(err, None)
    
    def mustDeleteOk(self, key, col, txn):
        '''
        @param txn: Transaction
        '''
        err = txn.Delete(key, col)
        self.assertEqual(err, None)
    
    def mustInsertOk(self, key, value, col, txn):
        '''
        @param txn: Transaction
        '''
        err = txn.Insert(key, value, col)
        self.assertEqual(err, None)
        
    def mustInsertErr(self, key, value, col, txn):
        '''
        @param txn: Transaction
        '''
        err = txn.Insert(key, value, col)
        self.assertNotEqual(err, None)
    
    
    def mustGetOk(self, key, expect, col, txn):
        '''
        @param txn: Transaction
        '''
        val, err = txn.Get(key, col)
        self.assertEqual(err, None)
        self.assertEqual(val, expect)
    
    def mustGetErr(self, key, col, txn):
        '''
        @param txn: Transaction
        '''
        _, err = txn.Get(key, col)
        self.assertNotEqual(err, None)
        
    def mustLockKeysOk(self, keys, col, txn):
        '''
        @param txn: Transaction
        '''
        err = txn.LockKeys(keys, col)
        self.assertEqual(err, None)
      
    def mustCommitOk(self, txn):
        '''
        @param txn: Transaction
        '''
        err = txn.Commit()
        self.assertEqual(err, None)
        
    def mustCommitErr(self, txn, expect):
        '''
        @param txn: Transaction
        '''
        err = txn.Commit()
        self.assertNotEqual(err, None)
        self.assertEqual(err, expect)
    
    def mustPrewriteOk(self, txn):
        '''
        @param txn: Transaction
        '''
        # 0. init mutations
        txn.us.WalkBuffer()            
        # 1. prewrite mutations
        err = txn.prewrite(Context())
        self.assertEqual(err, None)
  
    def mustPrewriteErr(self, txn):
        '''
        @param txn: Transaction
        '''
        # 0. init mutations
        txn.us.WalkBuffer()            
        # 1. prewrite mutations
        err = txn.prewrite(Context())
        self.assertNotEqual(err, None)
        
    def insertData(self, txn):
        for i in range(startIndex, testCount):
            val = self.encodeInt(i * indexStep)
            self.mustSetOk(val, val, col, txn)
          
    def mustDel(self, txn):
        for i in range(startIndex, testCount):
            val = self.encodeInt(i * indexStep)
            self.mustDeleteOk(val, col, txn)
            
    def encodeInt(self, n):
        return "%010d" % n
    
    def decodeInt(self, s):
        return int(s)
      
    def mustNotGet(self, txn):
        for i in range(startIndex, testCount):
            s = self.encodeInt(i * indexStep)
            self.mustGetOk(s, None, col, txn)
    
    def mustGet(self, txn):
        for i in range(startIndex, testCount):
            s = self.encodeInt(i * indexStep)
            self.mustGetOk(s, s, col, txn)
          
    def test_GetSet(self):
        txn = self.store.Begin()
        self.insertData(txn)
        self.mustGet(txn)
        self.mustCommitOk(txn)
    
        txn = self.store.Begin()
        self.mustGet(txn)
        self.mustDel(txn)
        self.mustCommitOk(txn)
   
    def test_Delete(self):
        txn = self.store.Begin()
        self.insertData(txn)
        self.mustDel(txn)
        self.mustNotGet(txn)
        self.mustCommitOk(txn)
    
        # Try get
        txn = self.store.Begin()
        self.mustNotGet(txn)
    
        # Insert again
        self.insertData(txn)
        self.mustCommitOk(txn)
    
        # Delete all
        txn = self.store.Begin()
        self.mustDel(txn)
        self.mustCommitOk(txn)
        
        txn = self.store.Begin()
        self.mustNotGet(txn)
        self.mustCommitOk(txn)
     
    def test_SetNil(self):
        txn = self.store.Begin()
        self.mustSetOk("1", None, col, txn)
        self.mustCommitOk(txn)
          
    def test_Rollback(self):
        txn = self.store.Begin()
        self.insertData(txn)
        self.mustGet(txn)
        txn.Rollback()
        
        txn = self.store.Begin()
        for i in range(startIndex, testCount):
            self.mustGetOk(self.encodeInt(i), None, col, txn)
        self.mustCommitOk(txn)

    
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        DckvTestCase('test_GetSet'),
        DckvTestCase('test_Delete'),
        DckvTestCase('test_SetNil'),
        DckvTestCase('test_Rollback'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)

#     unittest.main()
