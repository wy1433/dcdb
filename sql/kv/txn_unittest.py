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

nil = None

class TransactionTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TransactionTestCase, self).__init__(*args, **kwargs)
        
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
    
    def checkValues(self, m):
        txn = self.store.Begin()
        for col, kv in m.iteritems():
            for k, v in kv.iteritems():
                val, err = txn.Get(k, col)
                self.assertEqual(err, None)
                self.assertEqual(val, v)
                
    def mustCommit(self, m):
        txn = self.store.Begin()
        for col, kv in m.iteritems():
            for k, v in kv.iteritems():
                err = txn.Set(k, v, col)
                self.assertEqual(err, None)
        
        err = txn.Commit()
        self.assertEqual(err, None)
    
        self.checkValues(m)

    def test_CommitRollback(self):
        col1 = 'student.id.data'
        kv1 = {
            '1': '1',
            '2': '2',
            '3': '3',
            }
        col2 = 'student.name.data'
        kv2 = {
            '1': 'a',
            '2': 'b',
            '3': 'c',
            }
        m = {
            col1: kv1,
            col2: kv2,
            }
        # txn1 commit
        self.mustCommit(m)
        
        # start a txn
        txn = self.store.Begin()
        txn.Set('1', '11', col1)
        txn.Set('2', '21', col1)
        txn.Set('3', '31', col1)
        txn.Set('1', 'a1', col2)
        txn.Set('2', 'b1', col2)
        txn.Set('3', 'c1', col2)
    
        # txn2 commit, and txn2.commitTS > txn.startTS
        self.mustCommit({
            col1: {
                '3': '32',
                },
            col2: {
                 '3': 'c2',
                },
            }
        )
        
        # when txn commit, it will be abort on prewrite phase.
        err = txn.Commit()
        self.assertNotEqual(err, None)
    
        self.checkValues({
            col1: {
                '1': '1',
                '2': '2',
                '3': '32',
                },
            col2: {
                '1': 'a',
                '2': 'b',
                '3': 'c2',
                },
            }
        )   

    def test_PrewriteRollback(self):
        logger.debug('**********txn start***********')
        col = 'student.name.data'
        self.mustCommit({
             col: {
                'a': 'a0',
                'b': 'b0',
                },
            }
        )
    
        logger.debug('**********txn1 start***********')
        txn1 = self.store.Begin()
        err = txn1.Set("a", "a1", col)
        self.assertEqual(err, None)
        err = txn1.Set("b", "b1", col)
        self.assertEqual(err, None)
        txn1.us.WalkBuffer()
        err = txn1.prewrite()
        self.assertEqual(err, None)
        
        logger.debug('**********txn2 start***********')
        txn2 = self.store.Begin()
        txn2.isoLevel = kvrpcpb.RC
        v, err = txn2.Get("a", col)
        self.assertEqual(err, None)
        self.assertEqual(v, 'a0')
       
        logger.debug('**********txn1 retry***********')
        err = txn1.prewrite()
        if err != nil:
            # Retry.
            txn1 = self.store.Begin()
            err = txn1.Set("a", "a1", col)
            self.assertEqual(err, None)
            err = txn1.Set("b", "b1", col)
            self.assertEqual(err, None)
            txn1.us.WalkBuffer()
            err = txn1.prewrite()
            self.assertEqual(err, None)
        
        commitTS = txn1.store.GetTimestamp()
        txn1.commitTS = commitTS
        txn1.commit()
        self.assertEqual(err, None)
    
        logger.debug('**********txn3 start***********')
        txn3 = self.store.Begin()
        v, err = txn3.Get("b", col)
        self.assertEqual(err, None)
        self.assertEqual(v, 'b1')
    
    def test_CommitRetryable(self):
        col1 = 'student.id.data'
        col2 = 'student.name.data'
        col3 = 'student.age.data'
        txn1, txn2, txn3 = self.store.Begin(), self.store.Begin(), self.store.Begin()
        
        # txn1 locks "b"
        err = txn1.Set("b", "b1", col2)
        self.assertEqual(err, None)
        txn1.us.WalkBuffer()
        err = txn1.prewrite()
        self.assertEqual(err, None)
        
        # txn3 writes "c"
        err = txn3.Set("c", "c3", col3)
        self.assertEqual(err, None)
        err = txn3.Commit()
        self.assertEqual(err, None)
        
        # txn2 writes "a"(PK), "b", "c" on different regions.
        # "c" will return a retryable error.
        # "b" will get a Locked error first
        err = txn2.Set("a", "a2", col1)
        self.assertEqual(err, None)
        err = txn2.Set("b", "b2", col2)
        self.assertEqual(err, None)
        err = txn2.Set("c", "c2", col3)
        self.assertEqual(err, None)
        err = txn2.Commit()
        self.assertNotEqual(err, None)
        self.assertEqual(err, ErrRetry)
        logger.debug(err)
#         c.Assert(strings.Contains(err.Error(), txnRetryableMark), IsTrue)
    
    def isKeyLocked(self, key, col):
        txn = self.store.Begin()
        _, err = txn.Get(key, col)
        logger.debug('*******%s', err)
        return err is not None
    
    def test_CommitRetryable2(self):
        col1 = 'student.id.data'
        col2 = 'student.name.data'
        col3 = 'student.age.data'
            
        txn1, txn2 = self.store.Begin(), self.store.Begin()
        # txn2 writes "b"
        err = txn2.Set("b", "b2", col2)
        self.assertEqual(err, None)
        err = txn2.Commit()
#         self.assertEqual(err, None)
        # txn1 writes "a"(PK), "b", "c" on different regions.
        # "b" will return an error and cancel commit.
        err = txn1.Set("a", "a1", col1)
        self.assertEqual(err, None)
        err = txn1.Set("b", "b1", col2)
        self.assertEqual(err, None)
        err = txn1.Set("c", "c1", col3)
        self.assertEqual(err, None)
        err = txn1.Commit()
        self.assertNotEqual(err, None)
    
    def test_IllegalTso(self):
        col = 'student.id.data'
        txn = self.store.Begin()
        
        data = {
             col: {
                'name': 'aa',
                'age': '12',
                },
            }
    
        for col, kv in data.iteritems():
            for k, v in kv.iteritems():
                err = txn.Set(k, v, col)
                self.assertEqual(err, None)
        
        # make start ts bigger.
        txn.startTS = sys.maxint
        err = txn.Commit()
        self.assertNotEqual(err, None)
    
    def test_PrewritePrimaryKeyFailed(self):
        # commit (a,a1)
        col = 'student.id.data'
        txn1 = self.store.Begin()
        err = txn1.Set("a", "a1", col)
        self.assertEqual(err, None)
        err = txn1.Commit()
        self.assertEqual(err, None)
    
        # check a
        txn = self.store.Begin()
        v, err = txn.Get("a", col)
        self.assertEqual(err, None)
        self.assertEqual(v, 'a1')
    
        # set txn2's startTs before txn1's
        txn2 = self.store.Begin()
        txn2.startTS = txn1.startTS - 1
        err = txn2.Set("a", "a2", col)
        self.assertEqual(err, None)
        err = txn2.Set("b", "b2", col)
        self.assertEqual(err, None)
        # prewrite:primary a failed, b success
        err = txn2.Commit()
        self.assertNotEqual(err, None)
    
        # txn2 failed with a rollback for record a.
        txn = self.store.Begin()
        v, err = txn.Get("a", col)
        self.assertEqual(err, None)
        self.assertEqual(v, 'a1')
        v, err = txn.Get("b", col)
        self.assertEqual(v, None)
    
        # clean again, shouldn't be failed when a rollback already exist.
        err = txn2.cleanup()
        self.assertEqual(err, None)
    
        # check the data after rollback twice.
        txn = self.store.Begin()
        v, err = txn.Get("a", col)
        self.assertEqual(err, None)
        self.assertEqual(v, 'a1')
    
        # update data in a new txn, should be success.
        err = txn.Set("a", "a3", col)
        self.assertEqual(err, None)
        err = txn.Commit()
        self.assertEqual(err, None)
        # check value
        txn = self.store.Begin()
        v, err = txn.Get("a", col)
        self.assertEqual(err, None)
        self.assertEqual(v, 'a3')
    
    def mustGetValueOk(self, k, expect, col):
        '''GetValue from kv-server and not cleanup locks
        '''
        txn = self.store.Begin()
        pairs = txn.getValues([k], col)
        if len(pairs) == 0:
            self.assertEqual(None, expect)
        else:
            key, value, err = pairs[0]
            self.assertEqual(key, k)
            self.assertEqual(value, expect)
            self.assertEqual(err, None)
    
    def mustGetValueErr(self, k, col):
        '''GetValue from kv-server and not cleanup locks
        '''
        txn = self.store.Begin()
        pairs = txn.getValues([k], col)
        self.assertEqual(len(pairs), 1)
        self.assertNotEqual(pairs[0].Err, None)
            
    
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
    
    def test_Set(self):
        col = 'student.name.data'
        
        # test from local buffer
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustGetOk('a', 'a1', col, txn)
        self.mustSetOk('a', None, col, txn)
        self.mustGetOk('a', None, col, txn)
       
        # test from mvccDB
        txn = self.store.Begin()
        self.mustSetOk('a', 'a2', col, txn)
        self.mustCommitOk(txn)
        self.mustGetValueOk('a', 'a2', col)
        txn = self.store.Begin()
        self.mustSetOk('a', None, col, txn)
        self.mustCommitOk(txn)
        self.mustGetValueOk('a', None, col)
        

    def test_Insert(self):
        col = 'student.name.data'
        
        # test from local buffer
        txn = self.store.Begin()
        
        self.mustInsertOk('a', 'a1', col, txn)
        self.mustGetOk('a', 'a1', col, txn)
        
        self.mustInsertErr('a', None, col, txn)
        self.mustGetOk('a', 'a1', col, txn)
        
        self.mustSetOk('a', None, col, txn)
        self.mustGetOk('a', None, col, txn)
        
        self.mustInsertOk('a', 'a2', col, txn)
        self.mustGetOk('a', 'a2', col, txn)
       
        # test from mvccDB
        txn = self.store.Begin()
        self.mustInsertOk('a', 'a1', col, txn)
        self.mustCommitOk(txn)
        self.mustGetValueOk('a', 'a1', col)
        
        txn = self.store.Begin()
        self.mustInsertOk('a', None, col, txn)
        self.mustCommitErr(txn, ErrKeyExists)
        self.mustGetValueOk('a', 'a1', col)
        
        txn = self.store.Begin()
        self.mustSetOk('a', None, col, txn)
        self.mustCommitOk(txn)
        self.mustGetValueOk('a', None, col)
        
        txn = self.store.Begin()
        self.mustInsertOk('a', 'a2', col, txn)
        self.mustCommitOk(txn)
        self.mustGetValueOk('a', 'a2', col)
    
    def test_Delete(self):
        col = 'student.name.data'
        
        # ======================
        # test from local buffer
        ## set delete 
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustDeleteOk('a',  col, txn)
        self.mustGetOk('a', None, col, txn)
    
        ## set insert delete insert
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustInsertErr('a', 'a2', col, txn)
        self.mustGetOk('a', 'a1', col, txn)
        self.mustDeleteOk('a',  col, txn)
        self.mustInsertOk('a', 'a3', col, txn)
        self.mustGetOk('a', 'a3', col, txn)
       
        # ======================
        # test from local buffer
        ## set delete 
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustCommitOk(txn)
        
        txn = self.store.Begin()
        self.mustDeleteOk('a',  col, txn)
        self.mustCommitOk(txn)
        
        txn = self.store.Begin()
        self.mustGetOk('a', None, col, txn)
    
        ## set insert delete insert
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustCommitOk(txn)
        
        txn = self.store.Begin()
        self.mustInsertOk('a', 'a2', col, txn)
        self.mustCommitErr(txn, ErrKeyExists)
        
        txn = self.store.Begin()
        self.mustGetOk('a', 'a1', col, txn)
        
        txn = self.store.Begin()
        self.mustDeleteOk('a',  col, txn)
        self.mustCommitOk(txn)
        
        txn = self.store.Begin()
        self.mustInsertOk('a', 'a3', col, txn)
        self.mustCommitOk(txn)
        
        txn = self.store.Begin()
        self.mustGetOk('a', 'a3', col, txn)
        
    def test_LockKeys(self):
        col = 'student.name.data'
        # =========================
        # test from local buffer
        txn = self.store.Begin()
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustGetOk('a', None, col, txn)
       
        # =========================
        # test from mvccDB
        ## lock a nonexistent key
        txn = self.store.Begin()
        self.mustLockKeysOk(['a'],  col, txn)
        # a is locked in prewrite phase, and unlock in commit phase
        self.mustCommitOk(txn) 
        self.mustGetValueOk('a', None, col) 
        
        ## lock an existent key
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustCommitOk(txn)
        txn = self.store.Begin()
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustCommitOk(txn)
        
        ## lock the same key twice in one txn
        txn = self.store.Begin()
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustCommitOk(txn)
        
        ## lock the same key twice in different txns
        txn = self.store.Begin()
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustCommitOk(txn)
        
        txn = self.store.Begin()
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustCommitOk(txn)
       
        ## lock the same key twice in different txns without commit
        txn = self.store.Begin()
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustPrewriteOk(txn)
        
        txn = self.store.Begin()
        txn.MaxTxnTimeUse = 0 # disable lock_resolver
        self.mustLockKeysOk(['a'],  col, txn)
        self.mustPrewriteErr(txn)
    
       
    def test_Get(self):
        col = 'student.name.data'
        # =========================
        # test from local buffer
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustGetOk('a', 'a1', col, txn)
        self.mustSetOk('a', 'a2', col, txn)
        self.mustGetOk('a', 'a2', col, txn)
        
        # =========================
        # test from mvccDB
        ## set, get without locks
        txn = self.store.Begin()
        self.mustSetOk('a', 'a0', col, txn)
        self.mustCommitOk(txn) 
        self.mustGetOk('a', 'a0', col, txn)
        
        ## get local first
        txn = self.store.Begin()
        self.mustSetOk('a', 'a-local', col, txn)
        self.mustGetOk('a', 'a-local', col, txn)
        
        ## get with lock conflict
        txn1 = self.store.Begin()
        txn2 = self.store.Begin()
        txn3 = self.store.Begin()
        txn2.DefaultLockTTL = 6000 # set lock ttl = 6s
        self.mustSetOk('a', 'a2', col, txn2)
        self.mustPrewriteOk(txn2) # 'a' is  locked by txn2
        self.mustGetOk('a', 'a0', col, txn1) # txn1 is before txn2, so gets ok
        txn3.MaxTxnTimeUse = 0 # disable lock_resolver
        self.mustGetErr('a', col, txn3, ErrTxnTimeOut)
        
        txn3.MaxTxnTimeUse = 3000 # set txnTimeUse < lockTTL
        self.mustGetErr('a', col, txn3, ErrTxnTimeOut)
        
        txn3.MaxTxnTimeUse = 7000 # set txnTimeUse > lockTTL
        self.mustGetOk('a', 'a0', col, txn3) # txn2's lock is cleanup by txn3.
    
    
    def test_BatchGet(self):
        col1 = 'student.id.data'
        kv1 = {
            '1': '1',
            '2': '2',
            '3': '3',
            }
        col2 = 'student.name.data'
        kv2 = {
            '1': 'a',
            '2': 'b',
            '3': 'c',
            }
        m = {
            col1: kv1,
            col2: kv2,
            }
        # txn1 commit
        self.mustCommit(m)
        
        txn = self.store.Begin()
        ret_dict, err = txn.BatchGet(['1', '3', '5'], col2)
        self.assertEqual(err, None)
        expect = {
            '1' : 'a',
            '3' : 'c',
            }
        self.assertEqual(ret_dict, expect)
        
        # get local first
        txn = self.store.Begin()
        self.mustSetOk('1', '1-local', col2, txn)
        ret_dict, err = txn.BatchGet(['1', '3', '5'], col2)
        expect = {
            '1' : '1-local',
            '3' : 'c',
            }
        self.assertEqual(ret_dict, expect)
    
        
    def test_Scan(self):
        col1 = 'student.id.data'
        kv1 = {
            '-2': '1',
            '0': '2',
            '2': '3',
            }
        col2 = 'student.name.data'
        kv2 = {
            '1': 'a',
            '3': 'b',
            '5': 'c',
            }
        m = {
            col1: kv1,
            col2: kv2,
            }
        self.mustCommit(m)
        
        txn = self.store.Begin()
        ret_dict, err = txn.Scan('1','3',col=col2)
        self.assertEqual(err, None)
        expect = {
            '1' : 'a',
            }
        self.assertEqual(ret_dict, expect)
        
        ret_dict, err = txn.Scan('1','4',col=col2)
        expect = {
            '1' : 'a',
            '3' : 'b',
            }
        self.assertEqual(ret_dict, expect)
        
        # get local first
        txn = self.store.Begin()
        self.mustSetOk('3', '3-local', col2, txn)
        ret_dict, err = txn.Scan('1','4',col=col2)
        expect = {
            '1' : 'a',
            '3' : '3-local',
            }
        self.assertEqual(ret_dict, expect)
    
    def test_Commit(self):
        col = 'student.name.data'
        # ErrInvalidTxn
        txn = self.store.Begin()
        self.mustSetOk('a', 'a1', col, txn)
        self.mustSetOk('b', 'b1', col, txn)
        self.mustSetOk('c', 'c1', col, txn)
        txn.Close()
        self.mustCommitErr(txn, ErrInvalidTxn)
      
        # No Primary
        txn = self.store.Begin()
        self.mustCommitOk(txn)
          
        # =========================
        # test prewrite cases
        ## ErrKeyExists
        txn = self.store.Begin()
        self.mustInsertOk('a', 'a0', col, txn)
        self.mustCommitOk(txn)
        txn = self.store.Begin()
        self.mustInsertOk('a', 'a1', col, txn)
        self.mustCommitErr(txn, ErrKeyExists)
          
        ## ErrRetry
        txn1 = self.store.Begin()
        txn2 = self.store.Begin()
        self.mustSetOk('a', 'a2', col, txn1)
        self.mustCommitOk(txn1)
        self.mustSetOk('a', 'a3', col, txn2)
        self.mustCommitErr(txn2, ErrRetry)
          
        ## ErrLocked
        txn1 = self.store.Begin()
        txn2 = self.store.Begin()
        self.mustSetOk('a', 'a4', col, txn1)
        self.mustPrewriteOk(txn1)
        self.mustSetOk('a', 'a5', col, txn2)
        txn2.MaxTxnTimeUse = 0 # disable cleanup
        self.mustCommitErr(txn2, ErrRetry)
        self.mustCommitOk(txn1)
  
          
        ## ErrLocked and cleanup success
        txn1 = self.store.Begin()
        txn2 = self.store.Begin()
        self.mustSetOk('a', 'a6', col, txn1)
        self.mustPrewriteOk(txn1)
         
        self.mustSetOk('a', 'a7', col, txn2)
        txn2.MaxTxnTimeUse = 6000 # enable cleanup
        self.mustCommitOk(txn2)
        
        
        
        # =========================
        # test commit cases
        ## ErrRetry
        txn1 = self.store.Begin()
        txn2 = self.store.Begin()
        
        self.mustSetOk('a', 'a8', col, txn1)
        self.mustSetOk('b', 'b8', col, txn1)
        self.mustSetOk('c', 'c8', col, txn1)
        self.mustPrewriteOk(txn1)
         
        self.mustSetOk('a', 'a9', col, txn2)
        txn2.MaxTxnTimeUse = 6000 # enable cleanup
        self.mustCommitOk(txn2)
        
        ## ErrRetry lock is cleanup
        err = txn1.commit() # Primary key's lock a is cleanup by txn2
        self.assertEqual(err, ErrRetry)

        ## ErrRetry lock is cleanup and locked by another txn
        txn3 = self.store.Begin()
        self.mustSetOk('a', 'a10', col, txn3)
        self.mustPrewriteOk(txn3) # key b was locked by txn3

        err = txn1.commit() # Primary key's lock a is cleanup by txn2
        self.assertEqual(err, ErrRetry)
        
        
        
          
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        TransactionTestCase('test_CommitRollback'),
        TransactionTestCase('test_PrewriteRollback'),
        TransactionTestCase('test_CommitRetryable'),
        TransactionTestCase('test_CommitRetryable2'),
        TransactionTestCase('test_IllegalTso'),
        TransactionTestCase('test_PrewritePrimaryKeyFailed'),
        TransactionTestCase('test_Set'),
        TransactionTestCase('test_Insert'),
        TransactionTestCase('test_Delete'),
        TransactionTestCase('test_LockKeys'),
        TransactionTestCase('test_Get'),
        TransactionTestCase('test_BatchGet'),
        TransactionTestCase('test_Scan'),
        TransactionTestCase('test_Commit'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)

#     unittest.main()
