#!/usr/bin/env python
# -*-coding:utf-8 -*-

import time
import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
from util.error import *
from store.mvcc import Pair
from mylog import logger
from union_store import UnionStore

nil = None

# MaxTxnTimeUse = 600000  # Millisecond, 10 minutes
# DefaultLockTTL = 120000  # Millisecond, 2  minutes
# ScanLockInterval = 10  # second

# # speed up tests
MaxTxnTimeUse = 6000  # Millisecond
DefaultLockTTL = 3000  # Millisecond
ScanLockInterval = 1000  # Millisecond

# class Context():
# 
#     def __init__(self):
#         self.connID = 0
#         self.StartTS = 0
#         self.CommitTS = 0

pairs = list()


def ParserLockFromErr(err):
    '''
    @param err: ErrLocked
    @return: kvrpcpb.LockInfo
    '''
    if isinstance(err, ErrLocked):
        return kvrpcpb.LockInfo(
                primary_lock=err.primary,
                lock_version=err.startTS,
                key=err.key,
                lock_ttl=err.ttl
                )
    else:
        logger.error('type error, %s', type(err))
        return None


class Transaction(object):

    def __init__(self, store):
        '''
        @type store: DckvStore
        '''
        self.store = store  # for connection to meta and stores
        self.startTS = self.store.GetTimestamp()
        self.startTime = time.time()  # Monotonic timestamp for recording txn time consuming.
        self.commitTS = 0
        self.us = UnionStore(self.startTS)
        self.isoLevel = kvrpcpb.SI
        self.MaxTxnTimeUse = MaxTxnTimeUse
        self.DefaultLockTTL = DefaultLockTTL
        self.valid = True
        self.dirty = False

    def getValues(self, keys, col):
        '''Get values from kv.
        @param keys: keys to get.
        @param col: column name used to locate a mvccdb
        @return: Pairs: List(Pair)
        '''
        db = self.store.GetMvccDB(col)
        pairs = db.BatchGet(keys, self.startTS, self.isoLevel)
        return pairs
    
    def getLocks(self, keys, col):
        '''Get locks from kv.
        @param keys: keys to get.
        @param col: column name used to locate a mvccdb
        @return: List(LockInfo)
        '''
        db = self.store.GetMvccDB(col)
        locks = db.BatchGetLock(keys, self.startTS)
        return locks      
    
    def backoffAndMaybeCleanupLock(self, errs, col, mutations=None):
        '''There is a pending lock; try to clean it and wait
        @param errs: ErrLocked errs.
        @param col: column name used to locate a mvccdb
        @param mutations: for prewrite if mutations is not None, else for read
        @return  pairs, err : list(Pair), ErrTxnTimeOut
            pairs: List of Pair from mvccdb
            err: cleanup lock results.
                None: if lock errs is expired and cleanup success. 
                ErrTxnTimeOut: if cleanup Timeout
                ErrKeyExists: when prewrite and not ErrLocked err 
                ErrRetryable: when prewrite and not ErrLocked err 
        '''
        locks = [ParserLockFromErr(e) for e in errs]
        pairs = list()
        ls = locks
        maxtime = self.startTime + self.MaxTxnTimeUse / 1000
        now = time.time()
        num = 0
        ms = mutations
        while now < maxtime:
            logger.debug('now=%d, maxtime=%d', now, maxtime)
            # 1. try to resolve locks which is expired. all locks expired will be resolved
            ok = self.store.lockResolver.ResolveLocks(ls, col)
            logger.debug('resolvelocks=%s,col=%s', ok, col)
            # 2. if there are still some locks not expired, 
            # it is wise to sleep a while to wait locks cleanup.
            if not ok: 
                time.sleep(ScanLockInterval / 1000)
                
            # 3. try  again.
            if mutations:  # for Write
                # try to prewrite again.
                num += 1
                db = self.store.GetMvccDB(col)
                errs = db.Prewrite(mutations, self.us.primary, self.startTS, self.DefaultLockTTL)
                
                # update locks
                err, locked_errs = self.handle_prewrite_errs(errs)
             
                if err is None:  # prewrite ok
                    ls = list()
                    break
                elif err == ErrLockConflict:  # still locked
                    ls = [ParserLockFromErr(e) for e in locked_errs]
                else:  # encounters other err, such as ErrKeyExists , ErrRetryable
                    logger.debug('backoff=%d,locked=%d,return with prewrite err=%s,col=%s',
                                 num, len(locks), err.ERROR(), col)
                    return None, err
            else :  # for Read
                # try to get locks's value again.
                ks = [l.key for l in ls]
                ks.sort()
                pairs2 = self.getValues(ks, col)
                num += 1
                ls = list()
                for k, v, e in pairs2:
                    if e : 
                        ls.append(ParserLockFromErr(e))
                    else:
                        pairs.append((k, v))
                        
            if len(ls) == 0:  # all locks cleanup
                break
            now = time.time()
            
        logger.debug('backoff=%d,locks=%d,clean=%d,col=%s', num, len(locks), len(locks) - len(ls), col)
        if (len(ls) == 0):
            return pairs, None
        else:
            return None, ErrTxnTimeOut
        
    def Get(self, key, col):
        '''Get value by key from local membuffers or mvccdb.
        @param key: key to get.
        @param col: column name used to locate a mvccdb
        @return: value, err
        @rtype: str, ErrTxnTimeOut 
        '''
        # 1. find local store first
        value = self.us.Get(key, col)
        if value:
            logger.debug('k=%s,v=%s,col=%s,err=%d', key, value, col, 0)
            return value, None 
        
        # 2. if key not found in local store, then try to find it from remote mvcc store
        db = self.store.GetMvccDB(col)
        value, err = db.Get(key, self.startTS, self.isoLevel)
        locked = False
        cleanup = False
        
        # backoffAndMaybeCleanupLock
        if err:
            locked = True
            pairs, err = self.backoffAndMaybeCleanupLock([err], col)
            if not err:
                value = pairs[0][1]
                cleanup = True
        
        logger.debug('k=%s,v=%s,col=%s,err=%s,locked=%d,cleanup=%d',
                     key, value, col, err, locked, cleanup)
        return value, err
            
    # BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
    # The map will not contain nonexistent keys.
    def BatchGet(self, keys, col):
        '''gets  multi keys' value by keys
        @param keys: list(str) , keys to get.
        @param col: str, column name used to locate a mvccdb
        @rtype:  dict[str, str], ErrTxnTimeOut
        @return: pairs, err.
            pairs: is a map contains key/value pairs, 
            the map will not contain nonexistent keys.
            err:
                - None: if lock errs is expired and cleanup success. 
                - ErrTxnTimeOut: if cleanup Timeout
        '''
        # 1. find local store first
        ret_dict = self.us.BatchGet(keys, col)
#         logger.debug('keys=%s,col=%s,ret=%s', keys, col, ret_dict)
        
        ks = list()
#         for k, v in ret_dict.iteritems():
#             if v is None:
#                 ks.append(k)
        for k in keys:
            if k not in ret_dict:
                ks.append(k)
        
        if len(ks) == 0:
            logger.debug('keys=%s,col=%s,ret=%s', keys, col, ret_dict)
            return ret_dict, None
            
        # 2. if key not found in local store, then try to find it from remote mvcc store
        db = self.store.GetMvccDB(col)
        ks.sort()
        pairs = db.BatchGet(ks, self.startTS, self.isoLevel)
        locked = False
        cleanup = False
        
        # backoffAndMaybeCleanupLock
        errs = list()
        for p in pairs: 
            if p.Err:
                errs.append(p.Err)
            else:
                ret_dict[p.Key] = p.Value
        err = None        
        if len(errs):
            locked = True
            pairs, err = self.backoffAndMaybeCleanupLock(errs, col)
            if not err:
                cleanup = True
                for k, v in pairs:
                    ret_dict[k] = v
            else:
                ret_dict = None
            
        logger.debug('keys=%s,col=%s,ret=%s,err=%s,locked=%d,cleanup=%d',
                     keys, col, ret_dict, err, locked, cleanup)
        return ret_dict, err
     
    def Scan(self, startKey, endKey, limit=None, col=None):
        '''scan values by a key range [startKey, endKey)
        @param startKey: str , start key of the range
        @param endKey: str, end key of the range.
        @attention: a range is Left open right closed interval, such as: [startKey, endKey) 
        @param limit: limit number of results
        @param col: str, column name used to locate a mvccdb
        @rtype:  dict[str, str], ErrTxnTimeOut
        @return: pairs, err.
            pairs: is a map contains key/value pairs, 
            he map will not contain nonexistent keys.
            err:
                - None: if lock errs is expired and cleanup success. 
                - ErrTxnTimeOut: if cleanup Timeout
        '''
        if limit is None:
            limit = 0xFFFFFFFFFFFFFFFF  # MaxUint64
        
        # 1. find local store first
        ret_list = self.us.Scan(startKey, endKey, col)
        ret_dict = dict(ret_list)
            
        # 2. try to find it from remote mvcc store        
        db = self.store.GetMvccDB(col)
        pairs = db.Scan(startKey, endKey, limit, self.startTS, self.isoLevel)
        locked = False
        cleanup = False
        
        # backoffAndMaybeCleanupLock
        errs = list()
        for p in pairs:
            # local value first, only update when key nonexistent.
            if p.Key not in ret_dict: 
                if p.Err:
                    errs.append(p.Err)
                else:
                    ret_dict[p.Key] = p.Value
        err = None        
        if len(errs):
            locked = True
            pairs, err = self.backoffAndMaybeCleanupLock(errs, col)
            if not err:
                cleanup = True
                for k, v in pairs:
                    ret_dict[k] = v
            else:
                ret_dict = None
            
        logger.debug('start=%s,end=%s,col=%s,ret=%s,err=%s,locked=%d,cleanup=%d',
                     startKey, endKey, col, ret_dict, err, locked, cleanup)
        return ret_dict, err
    
    def Set(self, key, value, col):
        '''Set key/value into local memory buffer
        @param key: str
        @param value: str
        @param col: column name used to locate a kv-server
        '''
        self.dirty = True
        self.us.Set(key, value, col)
        logger.debug('k=%s,v=%s,col=%s', key, value, col)
    
    def Insert(self, key, value, col):
        '''Insert key/value into local memory buffer
        @param key: str
        @param value: str
        @param col: column name used to locate a kv-server
        @rtype: ErrKeyExists
        '''
        self.dirty = True
        err = self.us.Insert(key, value, col)
        logger.debug('k=%s,v=%s,col=%s,err=%s', key, value, col, err)
        return err
       
    def Delete(self, key, col):
        '''Delete set value=None into local memory buffer
        @param key: str
        @param col: column name used to locate a kv-server
        '''
        self.dirty = True
        self.us.Delete(key, col)
        logger.debug('k=%s,col=%s', key, col)
        
    # LockKeys tries to lock the entries with the keys in KV store.
    def LockKeys(self, keys, col):  # error
        '''lock kv-server's keys, locks will only exists on prewrite phase. and commit will cleanup
        the locks.
        if lock a same key again in one txn, it will success, but only the first is effective.
        if lock a same key again in another txn, it will be a lock confict err from kv-server
        and only the first lock is effective.
        @param keys: list(str) 
        @param col: str, column name used to locate a mvccdb
        '''
        self.dirty = True
        self.us.LockKeys(keys, col)
        logger.debug('keys=%s,col=%s', keys, col)
    
    def Commit(self, connID=0):
        ''' execute a 2pc commit to kv-server. if 2pc failed, cleanup txn.
        @param connID: int
        @return: BaseError
            None : success
            ErrInvalidTxn: txn is invalid.
            ErrKeyExists: if mutation op is Insert and the key already exists.
            ErrRetry: suggests that client may restart the txn again, and this txn is abort.
        '''
        logger.debug('***  2PC Start *** con=%d,startTS=%d', connID, self.startTS)
        err = self.twoPhaseCommit(connID)
        if err is not None:
            err1 = self.cleanup()
            if err1 != nil:
                logger.error("con:%d 2PC cleanup err: %s, tid: %d" % (connID, err1.ERROR(), self.startTS))
            else:
                logger.info("con:%d 2PC clean up done, tid: %d", connID, self.startTS)
        self.Close()
        logger.debug('*** 2PC End *** con=%d,startTS=%d,commitTS=%d,err=%s',
                     connID, self.startTS, self.commitTS, err)
        return err
    
    def twoPhaseCommit(self, connID=0):
        '''
        @param connID: int
        @return: err. 
            None : success
            ErrInvalidTxn: txn is invalid.
            ErrKeyExists: if mutation op is Insert And the key already exists.
            ErrRetry: suggests that client may restart the txn again, and this txn is abort.
        '''
        if not self.valid:
            return ErrInvalidTxn
        
        # 0. init mutations
        self.us.WalkBuffer()
        if self.us.primary is None:
            logger.debug("con:%d 2PC, no primary" % connID)
            return None
            
        # 1. prewrite mutations
        err = self.prewrite(connID)
        if err != nil:
            logger.debug("con:%d 2PC failed on prewrite: tid: %d, err:%s" % (
                connID, self.startTS, err))
            # prewrite only ErrKeyExists cann't retry now.
            if err != ErrKeyExists:
                err = ErrRetry
            return err
        
        # 2 commit mutations
        commitTS = self.store.GetTimestamp()
        # check commitTS
        if commitTS <= self.startTS:
            err = "con:%d Invalid transaction tso with start_ts=%d while commit_ts=%d" % (
                connID, self.startTS, commitTS)
            logger.error(err)
            return ErrInvalidTSO
        self.commitTS = commitTS
        if self.store.meta.IsExpired(self.startTS, MaxTxnTimeUse):
            logger.error("con:%d txn takes too much time, start: %d, commit: %d" % (
                connID, self.startTS, self.commitTS))
            return ErrRetry
        
        err = self.commit(connID)
        if err != nil:
            logger.debug("con:%d 2PC failed on commit, tid: %d, err:%s" % (connID, self.startTS, err))
            return err
        return nil
    
    def handle_prewrite_errs(self, errs):
        ''' handle mvcc Prewrite errs to txn's err.
        @param errs: BaseError. one of those errs:
            ErrKeyAlreadyExist : when op is Insert and key already exist.
            ErrLocked: wait to resolve lock
            ErrRetryable: restart txn
            None: success
        @return: err, locked_errs. 
        err should be one of those case:
            None: if all errs is None
            ErrKeyExists: if any of errs is ErrKeyAlreadyExist
            ErrRetryable: if any of errs is ErrRetryable, and no err is ErrKeyAlreadyExist
            ErrLockConflict : if any of errs is ErrLocked, and no err is ErrKeyAlreadyExist and ErrRetryable
        locked_errs: errs  if err is ErrLockConflict.
        '''
        ret = None
        locked_errs = list()
        for err in errs:
            # Check already exists error
            if isinstance(err, ErrKeyAlreadyExist):  # : :type err: ErrKeyAlreadyExist
                key = err.Key
                logger.debug("key: %s already exists", key)
                ret = ErrKeyExists
                break  # any of
            
            if isinstance(err, ErrRetryable):  # : :type err: ErrRetryable
                logger.debug("2PC prewrite encounters retryable err: %s", err.ERROR())
                ret = ErrRetry 
                continue  # any of but no ErrKeyExists err in the left errs.
            
            if ret is None:  # no other err yet.
                # Extract lock from key error
                if isinstance(err, ErrLocked):  # : :type err: ErrLocked
                    logger.debug("2PC prewrite encounters lock: %s", err.ERROR())
                    locked_errs.append(err)
                elif err is not None:
                    logger.error("2PC prewrite encounters unknown err: %s", err.ERROR())
        
        if ret:
            return ret, None
        else:
            if len(locked_errs):
                return ErrLockConflict, locked_errs
            else:
                return None, None
    
    def prewrite(self, connID=0):
        '''
        @type cs: ColumnStore
        @param connID: int
        @attention: add prefix locations for primary
        @return: err 
            None: success if all cs prewrite ok.
            ErrKeyExists: if mutation op is Insert And the key already exists.
            ErrRetry: suggests that client may restart the txn again, and this txn is abort.
            ErrTxnTimeOut: means that Lock conflict happened, but resolved locks timeout, txn is abort. 
        '''
        if self.us.primary is None:
            logger.debug("no primary")
            return None
        
        logger.debug('conn=%d, prewrite startTS = %d, primary = %s' % (connID, self.startTS, self.us.primary))
        
        db_errs = list()
        for cs in self.us.GetColumnStores():
            # TODO multi-threads.
            # in fact. commands should be requested in parallel mod, 
            # so we just simulate it's behavior in this version.
            # first collect all the result of requests. 
            # then give a total status of the method .
            if cs.primary is None:
                logger.debug("no mutations")
                continue
            
            db = self.store.GetMvccDB(cs.col)
            errs = db.Prewrite(cs.Mutations(), self.us.primary, self.startTS, self.DefaultLockTTL)
            err, locked_errs = self.handle_prewrite_errs(errs)
            if err == ErrLockConflict:
                _, err = self.backoffAndMaybeCleanupLock(locked_errs, cs.col, cs.Mutations())
                # ErrLockConflict will be replaced by ErrTxnTimeOut after backoff
            db_errs.append(err)
            
        # if All db prewrite success. err is None
        # else, err priority as below:
        # ErrKeyExists > ErrRetry = ErrTxnTimeOut
        err = None 
        for e in db_errs:
            if e == ErrKeyExists:
                err = ErrKeyExists
                break
            elif e is not None and err is None:
                err = e
        return err
           
    def commit(self,  connID=0):
        '''
        @type cs: ColumnStore
        @param connID: int
        @return: err 
            None: success if all cs commit ok.
            ErrRetry: suggests that client may restart the txn again, and this txn is abort.
        '''
        logger.debug('commit startTS = %d, primary = %s, commitTS=%d' % (
            self.startTS, self.us.primary, self.commitTS))
        
        e = None        
        for cs in self.us.GetColumnStores():
            if cs.primary is None:
                logger.debug("no mutations, connID: %d", connID)
                continue
            db = self.store.GetMvccDB(cs.col)
            err = db.Commit(cs.Keys(), self.startTS, self.commitTS)
            # the primary status represents txn's status
            if cs.primary == self.us.primary:  # if is primary
                if err:
                    logger.debug('commit err: %s', err.ERROR())
                    e = ErrRetry
        return e

    def cleanup(self):
        '''
        @type cs: ColumnStore
        @return: ErrRetry if cleanup failed. else None
        '''
        logger.debug('[txn cleanup] startTS = %d, primary = %s, commitTS=%d' % (
            self.startTS, self.us.primary, self.commitTS))
        for cs in self.us.GetColumnStores():
            if cs.primary is None:
                logger.debug("no mutations")
                continue
            db = self.store.GetMvccDB(cs.col)
            err = db.Rollback(cs.Keys(), self.startTS)
            if err:
                if not isinstance(err, ErrAlreadyCommitted):
                    logger.debug('cleanup err: %s', err.ERROR())
                    return ErrRetry
                else:
                    logger.warning('cleanup txn already commited, cleanup cancel')
                    return err
        return None
    
    def Close(self):
        self.valid = False
    
    # Rollback undoes the transaction operations to KV store.
    def Rollback(self):
        # reset union store
        self.us = UnionStore(self.startTS)
        self.valid = False

    # IsReadOnly checks if the transaction has only performed read operations.
    def IsReadOnly(self):  # bool
        return not self.dirty
    
    # StartTS returns the transaction start timestamp.
    def StartTS(self):  # uint64
        return self.startTS()

    # Valid returns if the transaction is valid.
    # A transaction become invalid after commit or rollback.
    def Valid(self):  # bool
        return self.valid
    
