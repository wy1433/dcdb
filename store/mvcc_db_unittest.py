import unittest
import os
import sys
sys.path.append("..")
import logging
import mylog
import conf
import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
from mvcc_db import *
from util.rwlock import RWLock

logger = logging.getLogger("dcdb.store")

def putMutations(*kvpairs):
    '''
    @type kvpairs:  list(str)
    @rtype: list(kvrpcpb.Mutation)
    '''
    mutations = list()
    for i in range(len(kvpairs))[::2]:
        mutation = kvrpcpb.Mutation()
        mutation.op = kvrpcpb.Put
        mutation.key = kvpairs[i]
        mutation.value = kvpairs[i + 1]
        mutations.append(mutation)
    return mutations


def lock(key, primary , ts):
    '''
    @type key:  string
    @type primary:  string
    @type ts:  uint64
    @rtype: kvrpcpb.LockInfo
    '''
    lock_info = kvrpcpb.LockInfo()
    lock_info.key = key
    lock_info.primary_lock = primary
    lock_info.lock_version = ts
    
    return lock_info

class MvccDBTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(MvccDBTestCase, self).__init__(*args, **kwargs)
        if False:
            self.store = MvccDB()
       
    @classmethod
    def setUpClass(cls):
        print('setUpClass')
#         print('init db ...')
#         cls.store = MvccDB()
#         cls.mu = RWLock()

    @classmethod
    def tearDownClass(cls):
#         cls.store.DeleteRange()
#         cls.store.Close()
#         os.system('rm -rf test.db')
#         print '\ndelete db ...'
        print('tearDownClass')

    def setUp(self): 
        print'\n------',self._testMethodName, '-------'
        print "setUp" , 'init db ...'
        self.store = MvccDB()

    def tearDown(self):
        del self.store
        os.system('rm -rf %s/store/test.db' % conf.dataPath)
        print 'tearDown', 'delete db ...'
        
    def mustGetNone(self, key, ts) :
        val, err = self.store.Get(key, ts, kvrpcpb.SI)
        self.assertEqual(err, None)
        self.assertEqual(val, None)
    
    
    def mustGetErr(self, key, ts) :
        val, err = self.store.Get(key, ts, kvrpcpb.SI)
        self.assertNotEqual(err, None)
        self.assertEqual(val, None)
    
    
    def mustGetOK(self, key, ts, expect) :
        val, err = self.store.Get(key, ts, kvrpcpb.SI)
        self.assertEqual(err, None)
        self.assertEqual(val, expect)
    
    
    def mustGetRC(self, key, ts, expect) :
        val, err = self.store.Get(key, ts, kvrpcpb.RC)
        self.assertEqual(err, None)
        self.assertEqual(val, expect)
    
    
    def mustPutOK(self, key, value, startTS, commitTS) :
        errs = self.store.Prewrite(putMutations(key, value), key, startTS, 0)
        for err in errs :
            self.assertEqual(err, None)
        
        err = self.store.Commit([key], startTS, commitTS)
        self.assertEqual(err, None)
    
    
    def mustDeleteOK(self, key, startTS, commitTS) :
        mutations = list()
        mutation = kvrpcpb.Mutation()
        mutation.op = kvrpcpb.Del
        mutation.key = key
        mutations.append(mutation)
        
        errs = self.store.Prewrite(mutations, key, startTS, 0)
        for err in errs :
            self.assertEqual(err, None)
        
        err = self.store.Commit([key], startTS, commitTS)
        self.assertEqual(err, None)
    
    
    def mustScanOK(self, start, limit, ts, *expect) :
        self.mustRangeScanOK(start, "", limit, ts, *expect)
    
    
    def mustRangeScanOK(self, start, end, limit, ts, *expect) :
        pairs = self.store.Scan(start, end, limit, ts, kvrpcpb.SI)
        logger.debug(pairs)
        logger.debug(expect)
        self.assertEqual(len(pairs)*2, len(expect))
        for i in range(len(pairs)) :
            self.assertEqual(pairs[i].Err, None)
            self.assertEqual(pairs[i].Key, expect[i*2])
            self.assertEqual(pairs[i].Value, expect[i*2+1])
    
    def mustReverseScanOK(self, end, limit, ts, *expect) :
        self.mustRangeReverseScanOK("", end, limit, ts, *expect)
    
    
    def mustRangeReverseScanOK(self, start, end, limit, ts, *expect) :
        pairs = self.store.ReverseScan(start, end, limit, ts, kvrpcpb.SI)
        self.assertEqual(len(pairs)*2, len(expect))
        for i in range(len(pairs)) :
            self.assertEqual(pairs[i].Err, None)
            self.assertEqual(pairs[i].Key, expect[i*2])
            self.assertEqual(pairs[i].Value, expect[i*2+1])
            
    def mustPrewriteOK(self, mutations, primary, startTS):
        errs = self.store.Prewrite(mutations, primary, startTS, 0)
        for err in errs:
            self.assertEqual(err, None)
    
    def mustCommitOK(self,keys, startTS, commitTS) :
        err = self.store.Commit(keys, startTS, commitTS)
        self.assertEqual(err, None)
    
    
    def mustCommitErr(self, keys, startTS, commitTS) :
        err = self.store.Commit(keys, startTS, commitTS)
        self.assertNotEqual(err, None)
    
    
    def mustRollbackOK(self, keys, startTS) :
        err = self.store.Rollback(keys, startTS)
        self.assertEqual(err, None)
    
    
    def mustRollbackErr(self, keys, startTS) :
        err = self.store.Rollback(keys, startTS)
        self.assertNotEqual(err, None)
    
    
    def mustScanLock(self, maxTs, expect) :
        locks, err = self.store.ScanLock(nil, nil, maxTs)
        self.assertEqual(err, None)
        self.assertEqual(locks, expect)
    
    
    def mustResolveLock(self, startTS, commitTS) :
        self.assertEqual(self.store.ResolveLock(nil, nil, startTS, commitTS), None)
    
    
    def mustBatchResolveLock(self, txnInfos) :
        self.assertEqual(self.store.BatchResolveLock(nil, nil, txnInfos), None)
    
    
    def mustDeleteRange(self, startKey, endKey) :
        err = self.store.DeleteRange(startKey, endKey)
        self.assertEqual(err, None)
    
    
    def test_Get(self) :
        self.mustGetNone("x", 10)
        self.mustPutOK("x", "x", 5, 10)
        self.mustGetNone("x", 9)
        self.mustGetOK("x", 10, "x")
        self.mustGetOK("x", 11, "x")
    
    
    def test_GetWithLock(self) :
        key = "key"
        value = "value"
        self.mustPutOK(key, value, 5, 10)
        mutations = list()
        mutation =  kvrpcpb.Mutation()
        mutation.op = kvrpcpb.Lock
        mutation.key = key
        mutations.append(mutation)
        
        # test with lock's type is lock
        self.mustPrewriteOK(mutations, key, 20)
        self.mustGetOK(key, 25, value)
        self.mustCommitOK([key], 20, 30)
    
        # test get with lock's max ts and primary key
        self.mustPrewriteOK(putMutations(key, "value2", "key2", "v5"), key, 40)
        self.mustGetErr(key, 41)
        self.mustGetErr("key2", lockVer)
#         self.mustGetOK(key, lockVer, "value")
    
    
    def test_Delete(self) :
        self.mustPutOK("x", "x5-10", 5, 10)
        self.mustDeleteOK("x", 15, 20)
        self.mustGetNone("x", 5)
        self.mustGetNone("x", 9)
        self.mustGetOK("x", 10, "x5-10")
        self.mustGetOK("x", 19, "x5-10")
        self.mustGetNone("x", 20)
        self.mustGetNone("x", 21)
    
    
    def test_CleanupRollback(self) :
        self.mustPutOK("secondary", "s-0", 1, 2)
        self.mustPrewriteOK(putMutations("primary", "p-5", "secondary", "s-5"), "primary", 5)
        self.mustGetErr("secondary", 8)
        self.mustGetErr("secondary", 12)
        self.mustCommitOK(["primary"], 5, 10)
        self.mustRollbackErr(["primary"], 5)
    
    @unittest.skip('skip test_ReverseScan')
    def test_ReverseScan(self) :
        # ver10: A(10) - B(_) - C(10) - D(_) - E(10)
        self.mustPutOK("A", "A10", 5, 10)
        self.mustPutOK("C", "C10", 5, 10)
        self.mustPutOK("E", "E10", 5, 10)
    
        def checkV10():
            self.mustReverseScanOK("Z", 0, 10)
            self.mustReverseScanOK("Z", 1, 10, "E", "E10")
            self.mustReverseScanOK("Z", 2, 10, "E", "E10", "C", "C10")
            self.mustReverseScanOK("Z", 3, 10, "E", "E10", "C", "C10", "A", "A10")
            self.mustReverseScanOK("Z", 4, 10, "E", "E10", "C", "C10", "A", "A10")
            self.mustReverseScanOK("E\x00", 3, 10, "E", "E10", "C", "C10", "A", "A10")
            self.mustReverseScanOK("C\x00", 3, 10, "C", "C10", "A", "A10")
            self.mustReverseScanOK("C\x00", 4, 10, "C", "C10", "A", "A10")
            self.mustReverseScanOK("B", 1, 10, "A", "A10")
            self.mustRangeReverseScanOK("", "E", 5, 10, "C", "C10", "A", "A10")
            self.mustRangeReverseScanOK("", "C\x00", 5, 10, "C", "C10", "A", "A10")
            self.mustRangeReverseScanOK("A\x00", "C", 5, 10)
        
        checkV10()
    
        # ver20: A(10) - B(20) - C(10) - D(20) - E(10)
        self.mustPutOK("B", "B20", 15, 20)
        self.mustPutOK("D", "D20", 15, 20)
    
        def checkV20() :
            self.mustReverseScanOK("Z", 5, 20, "E", "E10", "D", "D20", "C", "C10", "B", "B20", "A", "A10")
            self.mustReverseScanOK("C\x00", 5, 20, "C", "C10", "B", "B20", "A", "A10")
            self.mustReverseScanOK("A\x00", 1, 20, "A", "A10")
            self.mustRangeReverseScanOK("B", "D", 5, 20, "C", "C10", "B", "B20")
            self.mustRangeReverseScanOK("B", "D\x00", 5, 20, "D", "D20", "C", "C10", "B", "B20")
            self.mustRangeReverseScanOK("B\x00", "D\x00", 5, 20, "D", "D20", "C", "C10")
        
        checkV10()
        checkV20()
    
        # ver30: A(_) - B(20) - C(10) - D(_) - E(10)
        self.mustDeleteOK("A", 25, 30)
        self.mustDeleteOK("D", 25, 30)
    
        def checkV30() :
            self.mustReverseScanOK("Z", 5, 30, "E", "E10", "C", "C10", "B", "B20")
            self.mustReverseScanOK("C", 1, 30, "B", "B20")
            self.mustReverseScanOK("C\x00", 5, 30, "C", "C10", "B", "B20")
        
        checkV10()
        checkV20()
        checkV30()
    
        # ver40: A(_) - B(_) - C(40) - D(40) - E(10)
        self.mustDeleteOK("B", 35, 40)
        self.mustPutOK("C", "C40", 35, 40)
        self.mustPutOK("D", "D40", 35, 40)
    
        def checkV40():
            self.mustReverseScanOK("Z", 5, 40, "E", "E10", "D", "D40", "C", "C40")
            self.mustReverseScanOK("Z", 5, 100, "E", "E10", "D", "D40", "C", "C40")
        
        checkV10()
        checkV20()
        checkV30()
        checkV40()
    
    
    def test_Scan(self) :
        # ver10: A(10) - B(_) - C(10) - D(_) - E(10)
        self.mustPutOK("A", "A10", 5, 10)
        self.mustPutOK("C", "C10", 5, 10)
        self.mustPutOK("E", "E10", 5, 10)
    
        def checkV10():
            self.mustScanOK("", 0, 10)
            self.mustScanOK("", 1, 10, "A", "A10")
            self.mustScanOK("", 2, 10, "A", "A10", "C", "C10")
            self.mustScanOK("", 3, 10, "A", "A10", "C", "C10", "E", "E10")
            self.mustScanOK("", 4, 10, "A", "A10", "C", "C10", "E", "E10")
            self.mustScanOK("A", 3, 10, "A", "A10", "C", "C10", "E", "E10")
            # not supported key endwith '\x00'
            self.mustScanOK("A\x01", 3, 10, "C", "C10", "E", "E10")
            self.mustScanOK("C", 4, 10, "C", "C10", "E", "E10")
            self.mustScanOK("F", 1, 10)
            self.mustRangeScanOK("", "E", 5, 10, "A", "A10", "C", "C10")
            self.mustRangeScanOK("", "C\x01", 5, 10, "A", "A10", "C", "C10")
            self.mustRangeScanOK("A\x01", "C", 5, 10)
        
        checkV10()
    
        # ver20: A(10) - B(20) - C(10) - D(20) - E(10)
        self.mustPutOK("B", "B20", 15, 20)
        self.mustPutOK("D", "D20", 15, 20)
    
        def checkV20():
            self.mustScanOK("", 5, 20, "A", "A10", "B", "B20", "C", "C10", "D", "D20", "E", "E10")
            self.mustScanOK("C", 5, 20, "C", "C10", "D", "D20", "E", "E10")
            self.mustScanOK("D\x01", 1, 20, "E", "E10")
            self.mustRangeScanOK("B", "D", 5, 20, "B", "B20", "C", "C10")
            self.mustRangeScanOK("B", "D\x01", 5, 20, "B", "B20", "C", "C10", "D", "D20")
            self.mustRangeScanOK("B\x01", "D\x01", 5, 20, "C", "C10", "D", "D20")
        
        checkV10()
        checkV20()
    
        # ver30: A(_) - B(20) - C(10) - D(_) - E(10)
        self.mustDeleteOK("A", 25, 30)
        self.mustDeleteOK("D", 25, 30)
    
        def checkV30():
            self.mustScanOK("", 5, 30, "B", "B20", "C", "C10", "E", "E10")
            self.mustScanOK("A", 1, 30, "B", "B20")
            self.mustScanOK("C\x01", 5, 30, "E", "E10")
        
        checkV10()
        checkV20()
        checkV30()
    
        # ver40: A(_) - B(_) - C(40) - D(40) - E(10)
        self.mustDeleteOK("B", 35, 40)
        self.mustPutOK("C", "C40", 35, 40)
        self.mustPutOK("D", "D40", 35, 40)
    
        def checkV40():
            self.mustScanOK("", 5, 40, "C", "C40", "D", "D40", "E", "E10")
            self.mustScanOK("", 5, 100, "C", "C40", "D", "D40", "E", "E10")
        
        checkV10()
        checkV20()
        checkV30()
        checkV40()
    
    
    def test_BatchGet(self) :
        self.mustPutOK("k1", "v1", 1, 2)
        self.mustPutOK("k2", "v2", 1, 2)
        self.mustPutOK("k2", "v2", 3, 4)
        self.mustPutOK("k3", "v3", 1, 2)
        batchKeys = ("k1", "k2", "k3")
        pairs = self.store.BatchGet(batchKeys, 5, kvrpcpb.SI)
        for pair in pairs:
            self.assertEqual(pair.Err, None)
        
        self.assertEqual(pairs[0].Value, "v1")
        self.assertEqual(pairs[1].Value, "v2")
        self.assertEqual(pairs[2].Value, "v3")
    
    
    def test_ScanLock(self) :
        self.mustPutOK("k1", "v1", 1, 2)
        self.mustPrewriteOK(putMutations("p1", "v5", "s1", "v5"), "p1", 5)
        self.mustPrewriteOK(putMutations("p2", "v10", "s2", "v10"), "p2", 10)
        self.mustPrewriteOK(putMutations("p3", "v20", "s3", "v20"), "p3", 20)
    
        locks, err = self.store.ScanLock("a", "r", 12)
        self.assertEqual(err, None)
        
        self.assertEqual(locks, [
            lock("p1", "p1", 5),
            lock("p2", "p2", 10)]
        )
    
        self.mustScanLock(10, [
            lock("p1", "p1", 5),
            lock("p2", "p2", 10),
            lock("s1", "p1", 5),
            lock("s2", "p2", 10)
            ]
        )
    
    
    def test_CommitConflict(self) :
        # txn A want set x to A
        # txn B want set x to B
        # A prewrite.
        self.mustPrewriteOK(putMutations("x", "A"), "x", 5)
        # B prewrite and find A's lock.
        errs = self.store.Prewrite(putMutations("x", "B"), "x", 10, 0)
        self.assertNotEqual(errs[0], None)
        # B find rollback A because A exist too long.
        self.mustRollbackOK(["x"], 5)
        # if A commit here, it would find its lock removed, report error txn not found.
        self.mustCommitErr(["x"], 5, 10)
        # B prewrite itself after it rollback A.
        self.mustPrewriteOK(putMutations("x", "B"), "x", 10)
        # if A commit here, it would find its lock replaced by others and commit fail.
        self.mustCommitErr(["x"], 5, 20)
        # B commit succesself.
        self.mustCommitOK(["x"], 10, 20)
        # if B commit again, it will success because the key already committed.
        self.mustCommitOK(["x"], 10, 20)
    
    
    def test_ResolveLock(self) :
        self.mustPrewriteOK(putMutations("p1", "v5", "s1", "v5"), "p1", 5)
        self.mustPrewriteOK(putMutations("p2", "v10", "s2", "v10"), "p2", 10)
        self.mustResolveLock(5, 0)
        self.mustResolveLock(10, 20)
        self.mustGetNone("p1", 20)
        self.mustGetNone("s1", 30)
        self.mustGetOK("p2", 20, "v10")
        self.mustGetOK("s2", 30, "v10")
        self.mustScanLock(30, [])
    
    
    def test_BatchResolveLock(self) :
        self.mustPrewriteOK(putMutations("p1", "v11", "s1", "v11"), "p1", 11)
        self.mustPrewriteOK(putMutations("p2", "v12", "s2", "v12"), "p2", 12)
        self.mustPrewriteOK(putMutations("p3", "v13"), "p3", 13)
        self.mustPrewriteOK(putMutations("p4", "v14", "s3", "v14", "s4", "v14"), "p4", 14)
        self.mustPrewriteOK(putMutations("p5", "v15", "s5", "v15"), "p5", 15)
        txnInfos = {
            11: 0,
            12: 22,
            13: 0,
            14: 24
        }
        self.mustBatchResolveLock(txnInfos)
        self.mustGetNone("p1", 20)
        self.mustGetNone("p3", 30)
        self.mustGetOK("p2", 30, "v12")
        self.mustGetOK("s4", 30, "v14")
        self.mustScanLock(30, [
            lock("p5", "p5", 15),
            lock("s5", "p5", 15)
            ]
        )
        txnInfos = {
            15: 0
        }
        
        self.mustBatchResolveLock(txnInfos)
        self.mustScanLock(30, [])
    
    
    def test_RollbackAndWriteConflict(self) :
        self.mustPutOK("test", "test", 1, 3)
        errs = self.store.Prewrite(putMutations("lock", "lock", "test", "test1"), "test", 2, 2)
        self.mustWriteWriteConflict(errs, 1)
    
        self.mustPutOK("test", "test2", 5, 8)
    
        # simulate `getTxnStatus` for txn 2.
        err = self.store.Cleanup("test", 2)
        self.assertEqual(err, None)
    
        errs = self.store.Prewrite(putMutations("test", "test3"), "test", 6, 1)
        self.mustWriteWriteConflict(errs, 0)
    
    
    def test_DeleteRange(self) :
        for i in range(1, 6):
            key = str(i)
            value = "v" + key
            self.mustPutOK(key, value, 1+2*i, 2+2*i)
        
    
        self.mustScanOK("0", 10, 20, "1", "v1", "2", "v2", "3", "v3", "4", "v4", "5", "v5")
    
        self.mustDeleteRange("2", "4")
        self.mustScanOK("0", 10, 30, "1", "v1", "4", "v4", "5", "v5")
    
        self.mustDeleteRange("5", "5")
        self.mustScanOK("0", 10, 40, "1", "v1", "4", "v4", "5", "v5")
    
        self.mustDeleteRange("41", "42")
        self.mustScanOK("0", 10, 50, "1", "v1", "4", "v4", "5", "v5")
    
        self.mustDeleteRange("4\x01", "5\x01")
        self.mustScanOK("0", 10, 60, "1", "v1", "4", "v4")
    
        self.mustDeleteRange("0", "9")
        self.mustScanOK("0", 10, 70)
    
    
    def mustWriteWriteConflict(self, errs, i) :
        self.assertNotEqual(errs[i], None)
        self.assertEqual("write conflict" in errs[i].ERROR(), True)
    
    
    def test_RC(self) :
        self.mustPutOK("key", "v1", 5, 10)
        self.mustPrewriteOK(putMutations("key", "v2"), "key", 15)
        self.mustGetErr("key", 20)
        self.mustGetRC("key", 12, "v1")
        self.mustGetRC("key", 20, "v1")


class MarshalTestCase(unittest.TestCase):

    def test_MarshalmvccLock(self) :
        l = mvccLock(
            startTS=47,
            primary=b'abc',
            value=b'de',
            op=kvrpcpb.Put,
            ttl=444
            )
         
        bin, err = l.MarshalBinary()
        self.assertEqual(err, None)
     
        l1 = mvccLock()
        err = l1.UnmarshalBinary(bin)
        self.assertEqual(err, None)
     
        self.assertEqual(l.startTS, l1.startTS)
        self.assertEqual(l.op, l1.op)
        self.assertEqual(l.ttl, l1.ttl)
        self.assertEqual(l.primary, l1.primary)
        self.assertEqual(l.value, l1.value)
     
    def test_MarshalmvccValue(self) :
        v = mvccValue(
            valueType=typePut,
            startTS=42,
            commitTS=55,
            value=b'de'
            )
        bin, err = v.MarshalBinary()
        self.assertEqual(err, None)
     
        v1 = mvccValue()
        err = v1.UnmarshalBinary(bin)
        self.assertEqual(err, None)
     
        self.assertEqual(v.valueType, v1.valueType)
        self.assertEqual(v.startTS, v1.startTS)
        self.assertEqual(v.commitTS, v1.commitTS)
        self.assertEqual(v.value, v.value)
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        MvccDBTestCase('test_Get'), 
        MvccDBTestCase('test_GetWithLock'),
        MvccDBTestCase('test_Delete'),
        MvccDBTestCase('test_CleanupRollback'),
        MvccDBTestCase('test_Scan'),
        MvccDBTestCase('test_BatchGet'),
        MvccDBTestCase('test_ScanLock'),
        MvccDBTestCase('test_CommitConflict'),
        MvccDBTestCase('test_ResolveLock'),
        MvccDBTestCase('test_BatchResolveLock'),
        MvccDBTestCase('test_RollbackAndWriteConflict'),
        MvccDBTestCase('test_DeleteRange'),
        MvccDBTestCase('test_RC'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)     
#      
#     unittest.main()
