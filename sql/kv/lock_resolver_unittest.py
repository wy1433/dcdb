import unittest
import os
import sys
from mylog import logger

import conf
import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
from txn import *
from dckv import DckvStore
from sql.kv.dckv import DckvStore
from meta.infoschema import TableInfo, ColumnInfo, FieldType, IndexType

from lock_resolver import *

nil = None

class LockResolverTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(LockResolverTestCase, self).__init__(*args, **kwargs)
        
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
    
    def mustGetErr(self, key, col, txn, expect_err):
        '''
        @param txn: Transaction
        '''
        _, err = txn.Get(key, col)
        self.assertNotEqual(err, None)
        self.assertEqual(err, expect_err)
        
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
        err = txn.prewrite()
        self.assertEqual(err, None)
  
    def mustPrewriteErr(self, txn):
        '''
        @param txn: Transaction
        '''
        # 0. init mutations
        txn.us.WalkBuffer()            
        # 1. prewrite mutations
        err = txn.prewrite()
        self.assertNotEqual(err, None)
    
    def mustGetLocks(self, keys, col):
        txn = self.store.Begin()
        locks = txn.getLocks(keys, col)
        for key, lock in zip(keys, locks): 
            self.assertEqual(key, lock.key)
        return  locks

    def test_isAllExpired(self):
        col1 = 'student.id.data'
        col2 = 'student.name.data'
        
        # lock key '1'
        txn = self.store.Begin()
        txn.DefaultLockTTL = 3000
        self.mustSetOk('1', '1', col1, txn)
        self.mustSetOk('2', '2', col1, txn)
        self.mustPrewriteOk(txn)
        
        # lock key '2'
        txn = self.store.Begin()
        txn.DefaultLockTTL = 6000
        self.mustSetOk('1', 'a', col2, txn)
        self.mustSetOk('2', 'b', col2, txn)
        self.mustPrewriteOk(txn)
            
        locks1 = self.mustGetLocks(['1','2'], col1) # 3000ms 's lock
        locks2 = self.mustGetLocks(['1','2'], col2) # 6000ms 's lock
        
        self.assertFalse(self.store.lockResolver.isAllExpired(locks1))
        self.assertFalse(self.store.lockResolver.isAllExpired(locks2))
        
        # after 3s, locks1 is expired
        time.sleep(3)
        self.assertTrue(self.store.lockResolver.isAllExpired(locks1))
        self.assertFalse(self.store.lockResolver.isAllExpired(locks2))
        
        # after 6s, locks2 is expired
        time.sleep(3)
        self.assertTrue(self.store.lockResolver.isAllExpired(locks1))
        self.assertTrue(self.store.lockResolver.isAllExpired(locks2))
            
    
    def test_GetTxnStatus(self):
        col1 = 'student.id.data'
        
        # txn not commit
        # lock key '1' and not commit
        txn = self.store.Begin()
        self.mustSetOk('1', '1', col1, txn)
        self.mustSetOk('2', '2', col1, txn)
        self.mustPrewriteOk(txn)
        locks1 = self.mustGetLocks(['1','2'], col1)
        primary1 = locks1[0].primary_lock
        primary2 = locks1[1].primary_lock
        self.assertEqual(primary1, primary2)
        # primary key 'student.id.data.1' will rollback by GetTxnStatus
        # secondary key '2' lock will be leaked
        commitTS = self.store.lockResolver.GetTxnStatus(primary1, txn.startTS)
        self.assertEqual(commitTS, 0)
        
        # txn committed
        # now we retry to set again. and leaked lock key '2' will be resolved this time.
        txn = self.store.Begin()
        self.mustSetOk('1', '1', col1, txn)
        self.mustSetOk('2', '2', col1, txn)
        self.mustCommitOk(txn)
        commitTS = self.store.lockResolver.GetTxnStatus(primary1, txn.startTS)
        self.assertGreater(commitTS, 0)
        self.assertEqual(commitTS, txn.commitTS)
        
    def test_cleanUp(self):
        col1 = 'student.id.data'
        col2 = 'student.name.data'
        
        # product some locks
        locks1 = list()
        locks2 = list()
        txn = self.store.Begin()
        self.mustSetOk('1', 'id1', col1, txn)
        self.mustSetOk('1', 'name1', col2, txn)
        self.mustSetOk('2', 'id2', col1, txn)
        self.mustSetOk('2', 'name2', col2, txn)
        self.mustPrewriteOk(txn)
        ls = self.mustGetLocks(['1','2'], col1)
        locks1.extend(ls)
        ls = self.mustGetLocks(['1','2'], col2)
        locks2.extend(ls)
        
        txn = self.store.Begin()
        self.mustSetOk('3', 'id3', col1, txn)
        self.mustSetOk('3', 'name3', col2, txn)
        self.mustSetOk('4', 'id4', col1, txn)
        self.mustSetOk('4', 'name4', col2, txn)
        self.mustPrewriteOk(txn)
        ls = self.mustGetLocks(['3','4'], col1)
        locks1.extend(ls)
        ls = self.mustGetLocks(['3','4'], col2)
        locks2.extend(ls)
        
        txn = self.store.Begin()
        self.mustSetOk('1', 'id1', col1, txn)
        self.mustSetOk('1', 'name1', col2, txn)
        self.mustSetOk('2', 'id2', col1, txn)
        self.mustSetOk('2', 'name2', col2, txn)
        self.mustSetOk('3', 'id3', col1, txn)
        self.mustSetOk('3', 'name3', col2, txn)
        self.mustSetOk('4', 'id4', col1, txn)
        self.mustSetOk('4', 'name4', col2, txn)
        txn.MaxTxnTimeUse = 0 # disalbe cleanup
        self.mustCommitErr(txn, ErrRetry)
        
        self.store.lockResolver.cleanUp(locks1, col1)
        self.store.lockResolver.cleanUp(locks2, col2)
        
        txn = self.store.Begin()
        self.mustSetOk('1', 'id1', col1, txn)
        self.mustSetOk('1', 'name1', col2, txn)
        self.mustSetOk('2', 'id2', col1, txn)
        self.mustSetOk('2', 'name2', col2, txn)
        self.mustSetOk('3', 'id3', col1, txn)
        self.mustSetOk('3', 'name3', col2, txn)
        self.mustSetOk('4', 'id4', col1, txn)
        self.mustSetOk('4', 'name4', col2, txn)
        txn.MaxTxnTimeUse = 0 # disalbe cleanup
        self.mustCommitOk(txn)
        
    def test_ResolveLocks(self):
        col1 = 'student.id.data'
        col2 = 'student.name.data'
        
        # product some locks
        locks1 = list()
        locks2 = list()
        txn = self.store.Begin()
        txn.DefaultLockTTL = 3000
        self.mustSetOk('1', 'id1', col1, txn)
        self.mustSetOk('1', 'name1', col2, txn)
        self.mustSetOk('2', 'id2', col1, txn)
        self.mustSetOk('2', 'name2', col2, txn)
        self.mustPrewriteOk(txn)
        ls = self.mustGetLocks(['1','2'], col1)
        locks1.extend(ls)
        ls = self.mustGetLocks(['1','2'], col2)
        locks2.extend(ls)
        
        txn = self.store.Begin()
        txn.DefaultLockTTL = 6000
        self.mustSetOk('3', 'id3', col1, txn)
        self.mustSetOk('3', 'name3', col2, txn)
        self.mustSetOk('4', 'id4', col1, txn)
        self.mustSetOk('4', 'name4', col2, txn)
        self.mustPrewriteOk(txn)
        ls = self.mustGetLocks(['3','4'], col1)
        locks1.extend(ls)
        ls = self.mustGetLocks(['3','4'], col2)
        locks2.extend(ls)
    
        self.assertFalse(self.store.lockResolver.ResolveLocks(locks1, col1))
        self.assertFalse(self.store.lockResolver.ResolveLocks(locks2, col2))
        time.sleep(3)
        self.assertFalse(self.store.lockResolver.ResolveLocks(locks1, col1))
        self.assertFalse(self.store.lockResolver.ResolveLocks(locks2, col2))
        time.sleep(6)
        self.assertTrue(self.store.lockResolver.ResolveLocks(locks1, col1))
        self.assertTrue(self.store.lockResolver.ResolveLocks(locks2, col2))
        
        self.store.lockResolver.cleanUp(locks1, col1)
        self.store.lockResolver.cleanUp(locks2, col2)
        
        txn = self.store.Begin()
        self.mustSetOk('1', 'id1', col1, txn)
        self.mustSetOk('1', 'name1', col2, txn)
        self.mustSetOk('2', 'id2', col1, txn)
        self.mustSetOk('2', 'name2', col2, txn)
        self.mustSetOk('3', 'id3', col1, txn)
        self.mustSetOk('3', 'name3', col2, txn)
        self.mustSetOk('4', 'id4', col1, txn)
        self.mustSetOk('4', 'name4', col2, txn)
        txn.MaxTxnTimeUse = 0 # disalbe cleanup
        self.mustCommitOk(txn)
    
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        LockResolverTestCase('test_isAllExpired'),
        LockResolverTestCase('test_GetTxnStatus'),
        LockResolverTestCase('test_cleanUp'),
        LockResolverTestCase('test_ResolveLocks'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)

#     unittest.main()
