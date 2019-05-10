#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys
sys.path.append("..")
import rocksdb
# from typing import List
import logging
import mylog
import conf
from util.rwlock import RWLock
from util.error import *
from store.mvcc import *
import interface.gen_py.kvrpcpb_pb2 as kvrpcpb

# # format='%(asctime)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d %(message)s'
# format='%(levelname)s: %(asctime)s [%(filename)s:%(lineno)d|%(funcName)s] %(message)s'
# # DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a ' #配置输出时间的格式，注意月份和天数不要搞乱了
# DATE_FORMAT = '%H:%M:%S' #配置输出时间的格式，注意月份和天数不要搞乱了
# mylog.basicConfig(level=mylog.DEBUG, format = format, datefmt = DATE_FORMAT)
# # logger = mylog.getLogger(__name__)

logger = logging.getLogger("dcdb.store")

# FLAG_PUT  = b'P'
# FLAG_DELETE  = b'D';
# FLAG_LOCK  = b'L';

    
class ErrIterator(BaseError):

    def ERROR(self):
        return "invalid iterator"


class ErrWriteBatch(BaseError):

    def ERROR(self):
        return "invalid write batch"


# Iterator wraps iterator.Iterator to provide Valid() method.
class Iterator():

    def __init__(self, db, startKey=None, endKey=None):
        '''
        @type db: rocksdb.DB
        @param startKey: encodeKey with version of the start raw key
        @param endKey: encodeKey with version of the end raw key
        @attention: endKey not included. [startKey, endKey) 
        '''
        self.iterator = db.iteritems()
        if(startKey == None):
            self.iterator.seek_to_first()
        else:
            self.iterator.seek(startKey)
        self.valid = True
        self.startKey = startKey
        self.endKey = endKey
        self.Next()
        
    # Next moves the iterator to the next key/value pair.
    def Next(self):
        try:
            self.key, self.value = self.iterator.next()
            if self.endKey and self.key >= self.endKey:
                self.valid = False
            else:
                self.valid = True
        except StopIteration:
#             print 'StopIteration Exception'
            self.valid = False            
        return self.valid
        
    def Key(self):
        return self.key
    
    def Value(self):
        return self.value
    
#     def DecodeKey(self):
#         k, ver, err = mvccDecode(self.key)
#         return k
    
    # Valid returns whether the iterator is exhausted.
    def Valid(self):
        return self.valid

 
# iterDecoder tries to decode an Iterator value.
# If current iterator value can be decoded by this decoder, store the value and call iterator.Next(),
# Otherwise current iterator is not touched and returns Fasle.
class iterDecoder():

    def Decode(self):
        pass


class lockDecoder(iterDecoder):

    def __init__(self, lock=None, expectKey=None):
        '''
        @type lock: mvccLock
        @type expectKey: str
        '''
        self.lock = lock      
        self.expectKey = expectKey

    # Decode decodes the lock value if current iterator is at expectKey::lock.
    def Decode(self, iterator):
        '''
        @type iterator: Iterator 
        '''
#         logger.debug("iter.valid=%d,expectKey=%s" % (iterator.Valid(),self.expectKey))
        if not iterator.Valid():
            return False, None
            
        iterKey = iterator.Key()
        key, ver, err = mvccDecode(iterKey)
#         logger.debug('iterKey: key=%s, ver=%d, err=%s' %(key, ver, err) ) 
        if err != None:
            return False, err
        
        if key != self.expectKey:
            return False, None
        
        if ver != lockVer:
            return False, None
        
        lock = mvccLock()
        err = lock.UnmarshalBinary(iterator.Value())
        self.lock = lock
        logger.debug("mvccLock: key=%s,startTS=%d,value=%s,op=%d, primary=%s" % 
                     (key, lock.startTS, lock.value, lock.op, lock.primary))
        iterator.Next()
        return True, err
    

class valueDecoder(iterDecoder):

    def __init__(self, value=None, expectKey=None):
        '''
        @type value: mvccValue
        @type expectKey: str
        '''
        self.value = value      
        self.expectKey = expectKey

    # Decode decodes a mvcc value if iterator key is expectKey.
    def Decode(self, iterator):
        if not iterator.Valid():
            return False, None
        
        key, ver, err = mvccDecode(iterator.Key())
        if err != None:
            return False, err
        
        if key != self.expectKey:
            return False, None
        
        if ver == lockVer:
            return False, None
    
        value = mvccValue()
        value.UnmarshalBinary(iterator.Value())
        self.value = value
        logger.debug("key=%s, %s" % (key, repr(value)))
        iterator.Next()
        return True, None


class skipDecoder(iterDecoder):

    def __init__(self, currKey):
        '''
        @type currKey: str
        '''
        self.currKey = currKey
        
    # Decode skips the iterator as long as its key is currKey, the new key would be stored.
    def Decode(self, iterator):
        if not iterator.Valid():
            return False, None
        
        while iterator.Valid():
            key, _, err = mvccDecode(iterator.Key())
            if err != None :
                return False, err
                        
            if key != self.currKey:
                self.currKey = key
                return True, None
            
            iterator.Next()
        
        return False, None


def getValue(iterator, key, startTS, isoLevel):
    '''
    @type iterator: Iterator
    @type key: []byte
    @type startTS: uint64
    @type isoLevel: kvrpcpb.IsolationLevel
    @return: (value, err), when isoLevel is SI, and get lock in [startTS, None), otherwise err is None
    @rtype: str, ErrLocked 
    '''
    dec1 = lockDecoder(expectKey=key) 
    ok, err = dec1.Decode(iterator)
    logger.debug('lockDecoder[locked=%d, err=%s, key=%s, startTS=%d]' % (ok, err, key, startTS))
    if err:
        return None, None
    
    if ok and isoLevel == kvrpcpb.SI:
        startTS, err = dec1.lock.check(startTS, key)
        if err != None:
            return None, err
    
    dec2 = valueDecoder(expectKey=key)
    while iterator.Valid():
        ok, err = dec2.Decode(iterator)
        # should  never happened
        if err != None:
            # return None, err
            return None, None
        
        if not ok:
            break
        value = dec2.value
        if value.valueType == typeRollback:
            continue
        
        # Read the first committed value that can be seen at startTS.
        if value.commitTS <= startTS:
            if value.valueType == typeDelete: 
                return None, None
            
            return value.value, None
        
    return None, None


def prewriteMutation(db, batch, mutation, startTS, primary, ttl):
    '''
    @type db: rocksdb.DB 
    @type batch: rocksdb.WriteBatch
    @type mutation: kvrpcpb.Mutation
    @type startTS: uint64
    @type primary: []byte
    @type ttl: uint64
    @rtype: BaseError
    @return: 
        ErrLocked: when found lock conflict in lock , txn can sleep and wait lock resolver. 
        ErrRetryable: when found write conflict in value. txn must retry again.
    '''
    startKey = mvccEncode(mutation.key, lockVer)
    iterator = Iterator(db, startKey)

    dec = lockDecoder(expectKey=mutation.key)
    
    ok, err = dec.Decode(iterator)
    if err != nil:
        return err
    if ok:
        if dec.lock.startTS != startTS :
            err = dec.lock.lockErr(mutation.key)
            logger.warning(err.ERROR())
            return err
        logger.warning("lock already exists")
        return nil
 
    dec1 = valueDecoder(
        expectKey=mutation.key,
    )
    ok, err = dec1.Decode(iterator)
    
    if err != nil :
        return err
    
    # Note that it's a write conflict here, even if the value is a rollback one.
    if ok and dec1.value.commitTS >= startTS:
        err = ErrRetryable('write conflict')
        return err
    
    op = mutation.op
    if op == kvrpcpb.Insert:
        op = kvrpcpb.Put
    
    lock = mvccLock(
        startTS=startTS,
        primary=primary,
        value=mutation.value,
        op=op,
        ttl=ttl
    )
    writeKey = mvccEncode(mutation.key, lockVer)
    writeValue, _ = lock.MarshalBinary()
    batch.put(writeKey, writeValue)
    return nil


def commitKey(db, batch, key, startTS, commitTS):
    '''
    @type db: rocksdb.DB 
    @type batch: rocksdb.WriteBatch
    @type key: []byte
    @type startTS: uint64
    @type commitTS: uint64
    @rtype: ErrRetryable
    '''
    startKey = mvccEncode(key, lockVer)
    iterator = Iterator(db, startKey)
    
    dec = lockDecoder(
        expectKey=key
    )
    
    ok, err = dec.Decode(iterator)
#     logger.debug('lockDecoder[locked=%d, err=%s]' % (ok, err))
    if err != nil:
        return err
    
    if not ok or dec.lock.startTS != startTS :
        # If the lock of this transaction is not found, or the lock is replaced by
        # another transaction, check commit information of this transaction.
        c, ok, err1 = getTxnCommitInfo(iterator, key, startTS)
        if err1 != nil :
            return err1
        
        if ok and c.valueType != typeRollback :
            # c.valueType != typeRollback means the transaction is already committed, do nothing.
            return nil
        
        return ErrRetryable("txn not found")
    
    err = commitLock(batch, dec.lock, key, startTS, commitTS)
    if err != nil :
        return err
    
    return nil

    
def commitLock(batch , lock , key , startTS, commitTS):
    '''
    @type batch: rocksdb.WriteBatch
    @type lock: mvccLock
    @type key: []byte
    @type startTS: uint64
    @type commitTS: uint64
    @rtype: BaseError
    '''
    if lock.op != kvrpcpb.Lock :
        valueType = None
        if lock.op == kvrpcpb.Put :
            valueType = typePut
        else :
            valueType = typeDelete
        
        value = mvccValue(
            valueType=valueType,
            startTS=startTS,
            commitTS=commitTS,
            value=lock.value
            )
        
        writeKey = mvccEncode(key, commitTS)
        writeValue, err = value.MarshalBinary()
        if err != nil :
            return err
        
        batch.put(writeKey, writeValue)
    
    batch.delete(mvccEncode(key, lockVer))
    logger.debug("key=%s,value=%s,startTS=%s,commitTS=%s" % (key, lock.value, startTS, commitTS))
    return nil


def rollbackKey(db, batch, key, startTS):
    '''
    @type batch: rocksdb.WriteBatch
    @type key: []byte
    @type startTS: uint64
    @rtype: ErrAlreadyCommitted
    '''
    startKey = mvccEncode(key, lockVer)
    iterator = Iterator(db, startKey)
    if iterator.Valid() :
        dec = lockDecoder(expectKey=key)
        
        ok, err = dec.Decode(iterator)
        if err != nil :
            return err
        
        # If current transaction's lock exist.
        if ok and dec.lock.startTS == startTS :
            err = rollbackLock(batch, dec.lock, key, startTS)
            logger.debug("lock exist and rollback. key=%s,startTS=%s,err=%s" % (key, startTS, err))
            if err != nil :
                return err
            
            return nil
        # If current transaction's lock not exist.
        # If commit info of current transaction exist.
        c, ok, err = getTxnCommitInfo(iterator, key, startTS)
        if err != nil :
            return err
        if ok :
            # If current transaction is already committed.
            if c.valueType != typeRollback :
                logger.debug("txn is already committed. key=%s,startTS=%s" % (key, startTS))
                return ErrAlreadyCommitted(c.commitTS)
            
            # If current transaction is already rollback.
            logger.debug("txn is already rollback. key=%s,startTS=%s" % (key, startTS))
            return nil

    # If current transaction is not prewritted before.
    value = mvccValue(
        valueType=typeRollback,
        startTS=startTS,
        commitTS=startTS)
    
    writeKey = mvccEncode(key, startTS)
    writeValue, err = value.MarshalBinary()
    if err != nil :
        return err
    
    batch.put(writeKey, writeValue)
    
    logger.debug("key=%s,startTS=%s" % (key, startTS))
    return nil


def rollbackLock(batch, lock, key, startTS):
    tomb = mvccValue(
        valueType=typeRollback,
        startTS=startTS,
        commitTS=startTS
        )
    
    writeKey = mvccEncode(key, startTS)
    writeValue, err = tomb.MarshalBinary()
    if err != nil :
        return err
    
    batch.put(writeKey, writeValue)
    batch.delete(mvccEncode(key, lockVer))
    return nil


def getTxnCommitInfo(iterator, expectKey, startTS):
    while iterator.Valid() :
        dec = valueDecoder(
            expectKey=expectKey)
        
        ok, err = dec.Decode(iterator)
        if err != nil or not ok :
            return mvccValue(), ok, err

        if dec.value.startTS == startTS :
            return dec.value, True, nil
    
    return mvccValue(), False, nil


class MvccDB(object):
    '''
     Key layout:
     ...
     Key_lock        -- (0)
     Key_verMax      -- (1)
     ...
     Key_ver+1       -- (2)
     Key_ver         -- (3)
     Key_ver-1       -- (4)
     ...
     Key_0           -- (5)
     NextKey_lock    -- (6)
     NextKey_verMax  -- (7)
     ...
     NextKey_ver+1   -- (8)
     NextKey_ver     -- (9)
     NextKey_ver-1   -- (10)
     ...
     NextKey_0       -- (11)
     ...
     EOF
    '''
    
    def __init__(self, db_id=None, db_name='test.db'):
        self.db_id = db_id
        self.db_name = db_name
        opts = rocksdb.Options()
        opts.create_if_missing = True
        self.db = rocksdb.DB(conf.dataPath + "/store/" + db_name, opts)
#         column_families = [b"default", b"lock", b"write"]
#         self.db = rocksdb.DB('test.db', opts, column_families=column_families)
        self.mu = RWLock()
    
    def Get(self, key, startTS, isoLevel):
        '''
        @return: (str, err), value, ErrLocked
        '''
        self.mu.RLock()
        value, err = self.getValue(key, startTS, isoLevel)
        self.mu.RUnLock()
        logger.debug("req:{k=%s,startTS=%d,iso=%d},resp:{v=%s,err=%d}",
                     key, startTS, isoLevel, value, err is not None
                     )
        return value, err
    
    def getValue(self, key, startTS, isoLevel):
        startKey = mvccEncode(key, lockVer)
        it = Iterator(self.db, startKey)
        return getValue(it, key, startTS, isoLevel)
    
    def BatchGet(self, ks, startTS, isoLevel):
        '''
        @type ks: list(str)
        @type startTS: uint64
        @param isoLevel: kvrpcpb.IsolationLevel
        @rtype: list[Pair]
        '''
        self.mu.RLock()
        pairs = list()
        for k in ks:
            v, err = self.getValue(k, startTS, isoLevel)
            if v == None and err == None: 
                continue
            p = Pair(k, v, err)
            pairs.append(p)
            
        logger.debug("req:{ks=%s,startTS=%d,iso=%d},resp:{pairs=%s,err=%d}",
                     ks, startTS, isoLevel, pairs, err is not None
                     )
        self.mu.RUnLock()
        return pairs
    
    def Scan(self, startKey, endKey, limit, startTS, isoLevel):
        '''
        @type startKey, endKey: str
        @type limit: int
        @type startTS: uint64
        @type isoLevel: kvrpcpb.IsolationLevel
        @rtype: list(Pair)
        '''
        self.mu.RLock()
#         logger.debug("startKey=%s, endKey=%s, limit=%d, startTS=%d" % (startKey, endKey, limit, startTS))
        it = Iterator(self.db, mvccEncode(startKey, lockVer), mvccEncode(endKey, lockVer))
        err = None if it.Valid() else ErrIterator()
        if err != None:
            self.mu.RUnLock()         
            return {}
        
        currKey, _, _ = mvccDecode(it.Key())
        
        ok = True
        pairs = list()
        while len(pairs) < limit and ok :
            value, err = getValue(it, currKey, startTS, isoLevel)
            if err != None:
                p = Pair(currKey, None, err)
                pairs.append(p)
                
            if value != None:
                p = Pair(currKey, value, None)
                pairs.append(p)
    
            skip = skipDecoder(currKey)
            ok, err = skip.Decode(it)
            if err != None:
                logger.error("seek to next key error:", err.ERROR())
                break
            currKey = skip.currKey
        
        logger.debug("req:{startKey=%s,endKey=%s,limit=%d,startTS=%d},resp:{pairs=%s}",
                 startKey, endKey, limit, startTS, pairs
                 )
        self.mu.RUnLock()
        return pairs
    
    # ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
    def ReverseScan(self, startKey, endKey, limit, startTS, isoLevel):
        '''
        @todo: not implemnt yet.
        '''
        self.mu.RLock()
        pairs = list()
        self.mu.RUnLock()
        return pairs
      
    def Prewrite(self, mutations, primary, startTS, ttl):
        ''' prewrite mutations
        @type mutations: []*kvrpcpb.Mutation
        @type primary: []byte
        @type startTS: uint64
        @type ttl: uint64
        @rtype: list(BaseError)
        one of those errs:
            ErrKeyAlreadyExist : when op is Insert and key already exist.
            ErrLocked: wait to resolve lock
            ErrRetryable: restart txn
            None: success
        @attention: only anyError is False, mutations can apply to db.  
        '''
        self.mu.Lock()
        
        anyError = False
        batch = rocksdb.WriteBatch()
        errs = list()
        for m in mutations:
            # If the operation is Insert, check if key is exists at first.
            err = None
            if m.op == kvrpcpb.Insert:
                v, err = self.getValue(m.key, startTS, kvrpcpb.SI)
                if err != nil:
                    errs.append(err)
                    anyError = True
                    continue
                
                if v != nil:
                    err = ErrKeyAlreadyExist(
                        Key=m.key,
                    )
                    errs.append(err)
                    anyError = True
                    continue
            err = prewriteMutation(self.db, batch, m, startTS, primary, ttl)
            logger.debug("prewriteMutation, key=%s, value=%s, startTS=%s, err=%s" % (
                m.key, m.value, startTS, err))
            errs.append(err)
            if err != nil:
                anyError = True    
        
        if not anyError:
            try:
                self.db.write(batch)
            except Exception as e:
                print e
                
        self.mu.UnLock()
        return errs
    
    # Commit implements the MVCCStore interface.
    def Commit(self, keys, startTS, commitTS):
        '''
        @type keys: list(str)
        @type startTS: uint64
        @type commitTS: uint64
        @rtype: ErrRetryable
        '''
        self.mu.Lock()
       
        batch = rocksdb.WriteBatch()
        err = None
        for k in keys :
            err = commitKey(self.db, batch, k, startTS, commitTS)
            logger.info("commitKey, key=%s,startTS=%d,commitTS=%d, err=%s" % (k, startTS, commitTS, err))
            if err != nil :
                break
        
        if err is None:
            try:
                self.db.write(batch)
            except Exception as e:
                print e
                err = ErrWriteBatch()
                
        self.mu.UnLock()
        return err 
    
    # Rollback implements the MVCCStore interface.
    def Rollback(self, keys, startTS):
        '''
        @type keys: list(str)
        @type startTS: uint64
        '''
        self.mu.Lock()
    
        batch = rocksdb.WriteBatch()
        err = None
        for k in keys :
            err = rollbackKey(self.db, batch, k, startTS)
            logger.info("rollbackKey, key=%s, startTS=%s, err=%s" % (k, startTS, err))
            if err != nil :
                break

        if err is None:
            try:
                self.db.write(batch)
            except Exception as e:
                print e
                err = ErrWriteBatch()
                
        self.mu.UnLock()
        return err 
    
    # Cleanup implements the MVCCStore interface.
    def Cleanup(self, key, startTS):
        ''' rollbackKey try to get txtState for it's value's commitTS, Because only primary key
        can represent a txt state.
        @type keys: list(str)
        @type startTS: uint64
        @attention: the right way to use this interface is for expired primary key only,
        not secondary, not key un-expired.
        '''
        self.mu.Lock()
            
        batch = rocksdb.WriteBatch()
        err = rollbackKey(self.db, batch, key, startTS)
        logger.info("rollbackKey, key=%s, startTS=%s, err=%s" % (key, startTS, err))
        if err is None:
            try:
                self.db.write(batch)
            except Exception as e:
                print e
                err = ErrWriteBatch()
                
        self.mu.UnLock()
        return err 
    
    # ScanLock implements the MVCCStore interface.
    def ScanLock(self, startKey, endKey, maxTS):
        self.mu.RLock()
        iterator = Iterator(self.db, mvccEncode(startKey, lockVer), mvccEncode(endKey, lockVer)) 
        err = None if iterator.Valid() else ErrIterator()
        if err != None:
            self.mu.RUnLock()         
            return None, None
        currKey, _, _ = mvccDecode(iterator.Key())
        locks = list()
        while iterator.Valid() :
            dec = lockDecoder(expectKey=currKey)
            ok, err = dec.Decode(iterator)
            if err != nil :
                self.mu.RUnLock() 
                return nil, err

            if ok and dec.lock.startTS <= maxTS :
                lock = kvrpcpb.LockInfo(
                    primary_lock=dec.lock.primary,
                    lock_version=dec.lock.startTS,
                    key=currKey,
                    lock_ttl=dec.lock.ttl
                    )
                locks.append(lock)
                
            skip = skipDecoder(currKey=currKey)
            _, err = skip.Decode(iterator)
            if err != nil :
                self.mu.RUnLock()
                return nil, err
            
            currKey = skip.currKey
        
        logger.debug("req:{startKey=%s,endKey=%s,maxTS=%d},resp:{locks=%s}",
                     startKey, endKey, maxTS, locks
                     )
        self.mu.RUnLock()
        return locks, nil
    
    def getTxnCommitTS(self, key, startTS):
        '''
        @summary: key must be a primary key
        @see: http://mysql.taobao.org/monthly/2018/11/02/
        @return: return txn(startTS)'s commitTS, if commitTS==0, indicates txn not commited yet.
        '''
        startKey = mvccEncode(key, lockVer)
        iterator = Iterator(self.db, startKey)
        if iterator.Valid() :
            dec = lockDecoder(expectKey=key)    
            ok, err = dec.Decode(iterator)
                        
            # If commit info of current transaction exist.
            c, ok, err = getTxnCommitInfo(iterator, key, startTS)  # : :type c: mvccValue
            if err != nil :
                return err
            if ok :
                # If current transaction is already committed.
                if c.valueType != typeRollback :
                    return c.commitTS
                
                # If current transaction is already rollback.
                return 0
        return 0
    
    def getLock(self, key, maxTS):
        startKey = mvccEncode(key, lockVer)
        iterator = Iterator(self.db, startKey)
        lock = None
        if iterator.Valid() :
            dec = lockDecoder(expectKey=key)
            ok, err = dec.Decode(iterator)
            if err != nil :
                self.mu.RUnLock()
                return err, nil
            
            if ok and dec.lock.startTS <= maxTS :
                lock = kvrpcpb.LockInfo(
                    primary_lock=dec.lock.primary,
                    lock_version=dec.lock.startTS,
                    key=key,
                    lock_ttl=dec.lock.ttl
                    )
        return lock
    
    def GetLock(self, key, maxTS):
        '''Get LockInfo by key.
        @param key: str. key's lock
        @param maxTS: locked which is before MaxTS
        @return: kvrpcpb.LockInfo if lock exists before maxTS, else None
        '''
        self.mu.RLock()
        lock = self.getLock(key, maxTS)
        self.mu.RUnLock()
        logger
        return lock
    
    def BatchGetLock(self, keys, maxTS):
        '''Get LockInfo list by keys.
        @param keys: list(str), list of key to find lock
        @param maxTS: locked which is before MaxTS
        @return: list(kvrpcpb.LockInfo),  append LockInfo if lock exists or None to list.
        '''
        self.mu.RLock()
        locks = list()
        for key in keys:
            lock = self.getLock(key, maxTS)
            logger.debug("getLock, key=%s, maxTS=%s, lock=%s" % (key, maxTS, lock))
            locks.append(lock)        
        self.mu.RUnLock()
        return locks
    
    # ResolveLock implements the MVCCStore interface.
    def ResolveLock(self, startKey, endKey, startTS, commitTS):
        self.mu.Lock()
        iterator = Iterator(self.db, mvccEncode(startKey, lockVer), mvccEncode(endKey, lockVer))
        err = None if iterator.Valid() else ErrIterator()
        if err != None:
            self.mu.UnLock()         
            return None
        currKey, _, _ = mvccDecode(iterator.Key())
    
        batch = rocksdb.WriteBatch()
        while iterator.Valid() :
            dec = lockDecoder(expectKey=currKey)
            ok, err = dec.Decode(iterator)
            if err != nil :
                self.mu.UnLock() 
                return err
            
            if ok and dec.lock.startTS == startTS :
                if commitTS > 0 :
                    err = commitLock(batch, dec.lock, currKey, startTS, commitTS)
                    logger.debug("commitLock. key=%s,startTS=%d,commitTS=%d,err=%s",
                                    currKey, dec.lock.startTS, commitTS, err)
                else :
                    err = rollbackLock(batch, dec.lock, currKey, startTS)
                    logger.debug("rollbackLock. key=%s,startTS=%d,err=%s",
                                 currKey, dec.lock.startTS, err)
                
                if err != nil :
                    self.mu.UnLock()
                    return err
    
            skip = skipDecoder(currKey=currKey)
            _, err = skip.Decode(iterator)
            if err != nil :
                self.mu.UnLock()
                return err
            
            currKey = skip.currKey

        try:
            self.db.write(batch)
        except Exception as e:
            print e
            err = ErrWriteBatch()
        finally:
            self.mu.UnLock()
            return err
    
    # BatchResolveLock implements the MVCCStore interface.
    def BatchResolveLock(self, startKey, endKey, txnInfos):
        '''
        @type txnInfos: dict[int, int], dict[startTS]=commitTS
        '''
        self.mu.Lock()
    
        iterator = Iterator(self.db, mvccEncode(startKey, lockVer), mvccEncode(endKey, lockVer))
        err = None if iterator.Valid() else ErrIterator()
        if err != None:
            self.mu.UnLock()         
            return None
        currKey, _, _ = mvccDecode(iterator.Key())
    
        batch = rocksdb.WriteBatch()
        while iterator.Valid() :
            dec = lockDecoder(expectKey=currKey)
            ok, err = dec.Decode(iterator)
            if err != nil :
                self.mu.UnLock()
                return err
            
            if ok :
                if  txnInfos.has_key(dec.lock.startTS) :
                    commitTS = txnInfos[dec.lock.startTS]
                    if commitTS > 0 :
                        err = commitLock(batch, dec.lock, currKey, dec.lock.startTS, commitTS)
                        logger.debug("commitLock. key=%s,startTS=%d,commitTS=%d,err=%s",
                                     currKey, dec.lock.startTS, commitTS, err)
                    else :
                        err = rollbackLock(batch, dec.lock, currKey, dec.lock.startTS)
                        logger.debug("rollbackLock. key=%s,startTS=%d,err=%s",
                                     currKey, dec.lock.startTS, err)
                    
                    if err != nil :
                        self.mu.UnLock()
                        return err
                    
            skip = skipDecoder(currKey=currKey)
            _, err = skip.Decode(iterator)
            if err != nil :
                self.mu.UnLock()
                return err
            
            currKey = skip.currKey
        
        try:
            self.db.write(batch)
        except Exception as e:
            print e
            err = ErrWriteBatch()
        finally:
            self.mu.UnLock()
            return err
    
    # DeleteRange implements the MVCCStore interface.
    def DeleteRange(self, startKey=None, endKey=None):
        self.mu.Lock()
        batch = rocksdb.WriteBatch()
    
        iterator = Iterator(self.db, startKey=mvccEncode(startKey, lockVer), endKey=mvccEncode(endKey, lockVer))
        err = None if iterator.Valid() else ErrIterator()
        if err != None:
            self.mu.UnLock()         
            return None
        
        while iterator.Valid():
            batch.delete(iterator.Key())
            iterator.Next()
                    
        try:
            self.db.write(batch)
        except Exception as e:
            print e
            err = ErrWriteBatch()
        finally:
            self.mu.UnLock()
            return err
        
    # Close calls leveldb's Close to free resources.
    def Close(self):
        del self.db

    
def print_db(db_name):
    opts = rocksdb.Options()
    opts.create_if_missing = True
    db = rocksdb.DB(db_name, opts)
    it = db.iteritems()
    print list(it)


if __name__ == '__main__':
    print_db('test.db')
