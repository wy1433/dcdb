#!/usr/bin/env python
# -*-coding:utf-8 -*-

import time
import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
from util.error import ErrLocked, MaxUint64, ErrAlreadyCommitted
from mylog import logger
from interface.gen_py.kvrpcpb_pb2 import Normal

nil = None

ScanLockInterval = 10  # second

# LockInfo = namedtuple('LockInfo', ['Key', 'Primary','TxnID', 'TTL'])

class LockResolver():

    def __init__(self, store):
        '''
        @param store: DckvStore
        '''
        self.store = store
    
    def cleanUp(self, locks, col):
        txnInfos = dict() #: : type txnInfos:dict[int, int], dict[startTS]=commitTS
        startKey = None
        endKey = None
        for l in locks:  # : :type l: kvrpcpb.LockInfo
            if l.lock_version not in txnInfos:
                commitTS = self.GetTxnStatus(l.primary_lock, l.lock_version)
                txnInfos[l.lock_version] = commitTS
            key = l.key
            if startKey is None or key < startKey:
                startKey = key
            if endKey is None or key > endKey:
                endKey = key
                
        logger.debug('CleanUp Start, db=%s,startKey=%s,endKey=%s', col, startKey, endKey)
        db = self.store.GetMvccDB(col)
        err = db.BatchResolveLock(startKey, endKey+'\1', txnInfos)
        logger.info("CleanUp End, db=%s, txnInfos=%s, err=%s", col, txnInfos, err)
        return False if err else True
                     
    def isAllExpired(self, locks):
        for l in locks: #: : type l: kvrpcpb.LockInfo
            if not self.store.meta.IsExpired(l.lock_version, l.lock_ttl):
                return False
        return True
    
    def ResolveLocks(self, locks, col):
        '''
        @param locks: list(kvrpcpb.LockInfo)
        '''
        if self.isAllExpired(locks):
            logger.debug('all locks expired, cleanup start...')
            return self.cleanUp(locks, col)
        else:
            return False
        
    def GetTxnStatus(self, primary, startTS):
        '''GetTxnStatus queries mvcc-server for a txn's status (commit/rollback).
        If the primary key is still locked, it will launch a Rollback to abort it.
        To avoid unnecessarily aborting too many txns, it is wiser to wait a few
        seconds before calling it after Prewrite.
        @param primary: str. txn's primary key. the key is encode.
        @param startTS: int. txn's startTS
        @return: int. txn's commitTS which represents the status of the txn.
            if commitTS > 0: txn has already commited, locks in the same txn should be rollforword
            if commitTS = 0: txn has not commited yet, locks in the same txn should be rollback
        '''
        db_name, primary_key = self.store.meta.db_info.DecodePrimary(primary)
        db = self.store.GetMvccDB(db_name)
        err = db.Cleanup(primary_key, startTS)
        commitTS = 0
        if isinstance(err, ErrAlreadyCommitted):
            commitTS = err.commitTS
        logger.debug('primary=%s,startTS=%d,commitTS=%d', primary, startTS, commitTS)
        return commitTS
    
