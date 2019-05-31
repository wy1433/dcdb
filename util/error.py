#!/usr/bin/env python
# -*-coding:utf-8 -*-
MaxUint64 = 0xFFFFFFFFFFFFFFFF  # MaxUint64


class BaseError(object):
    
    def __init__(self, msg = None):
        self.msg = msg
        
    def ERROR(self):
        return self.msg
    
    def __repr__(self):
        return "%s:%s" % (self.__class__.__name__, self.ERROR())
    
    def __str__(self):
        return "%s:%s" % (self.__class__.__name__, self.ERROR())
    
    
class DBError(BaseError):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
    # Error formats the lock to a string.
    def ERROR(self):
        return "DBError. code=%d, msg=%d" %(self.code, self.msg)
      
######ErrCode######
'''
code={2:app}{2:module}{2:class}{2:method}
'''
####sql=01******
###sql.kv = 0101****
##sql.kv.dckv = 010101**
ErrSqlKvTxnGet = DBError(01010101, "Get Failed")
ErrParser = DBError(100000, "Parser Failed")
ErrTxnAlreadyExists = DBError(100001, "NewTxn Failed, commit or rollback first")
ErrExecutor = DBError(100002, "Executor Failed")

# ErrInvalidSessionID = DBError(100003, "invalid session id")


class KvError(BaseError):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
    # Error formats the lock to a string.
    def ERROR(self):
        return "KvError. code=%d, msg=%s" %(self.code, self.msg)
    
    
ErrInvalidSql = KvError(101, "invalid sql string error")
    
# ErrLocked is returned when trying to Read/Write on a locked key. Client should
# backoff or cleanup the lock then retry.
class ErrLocked(BaseError):   
    def __init__(self, key, primary, startTS, ttl):
        self.key = key
        self.primary = primary
        self.startTS = startTS
        self.ttl = ttl
        
    # Error formats the lock to a string.
    def ERROR(self):
        return "key is locked, key: %s, primary: %s, startTS: %d" %(self.key, self.primary, self.startTS)


# KV error codes.
# codeClosed                                    = 1
# codeNotExist                                  = 2
# codeConditionNotMatch                         = 3
codeLockConflict                              = 4
# codeLazyConditionPairsNotMatch                = 5
codeRetry                                     = 6
# codeCantSetNilValue                           = 7
codeInvalidTxn                                = 8
codeNotCommitted                              = 9
# codeNotImplemented                            = 10
# codeTxnTooLarge                               = 11
# codeEntryTooLarge                             = 12
codeKeyExists                                 = 13
codeTxnTimeOut                                = 14
codeInvalidTSO                                = 15
codeSessionMaxSize                            = 16

# ErrClosed is used when close an already closed txn.
# ErrClosed = KvError(codeClosed, "Error: Transaction already closed")
# ErrNotExist is used when try to get an entry with an unexist key from KV store.
# ErrNotExist = KvError(codeNotExist, "Error: key not exist")
# ErrConditionNotMatch is used when condition is not met.
# ErrConditionNotMatch = KvError(codeConditionNotMatch, "Error: Condition not match")
# ErrLockConflict is used when try to lock an already locked key.
ErrLockConflict = KvError(codeLockConflict, "Error: Lock conflict")
# ErrLazyConditionPairsNotMatch is used when value in store differs from expect pairs.
# ErrLazyConditionPairsNotMatch = KvError(codeLazyConditionPairsNotMatch, "Error: Lazy condition pairs not match")

# ErrRetryable is used when KV store occurs RPC error or some other
# errors which SQL layer can safely retry.
ErrRetry = KvError(codeRetry, "Error: KV error safe to retry")
# ErrCannotSetNilValue is the error when sets an empty value.
# ErrCannotSetNilValue = KvError(codeCantSetNilValue, "can not set nil value")
# ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
ErrInvalidTxn = KvError(codeInvalidTxn, "invalid transaction")
# ErrTxnTooLarge is the error when transaction is too large, lock time reached the maximum value.
# ErrTxnTooLarge = KvError(codeTxnTooLarge, "transaction is too large")
# ErrEntryTooLarge is the error when a key value entry is too large.
# ErrEntryTooLarge = KvError(codeEntryTooLarge, "entry is too large")

# ErrNotCommitted is the error returned by CommitVersion when this
# transaction is not committed.
ErrNotCommitted = KvError(codeNotCommitted, "this transaction has not committed")

# ErrKeyExists returns when key is already exist.
ErrKeyExists = KvError(codeKeyExists, "key already exist")
# ErrNotImplemented returns when a function is not implemented yet.
# ErrNotImplemented = KvError(codeNotImplemented, "not implemented")

ErrTxnTimeOut = KvError(codeTxnTimeOut, "txn timeout to resolve locks")

ErrInvalidTSO = KvError(codeTxnTimeOut, "txn invalid tso error")


ErrSessionMaxSize = KvError(codeSessionMaxSize, "session max size error")


# ErrKeyAlreadyExist is returned when key exists but this key has a constraint that
# it should not exist. Client should return duplicated entry error.
class ErrKeyAlreadyExist(BaseError):
    def __init__(self, Key):
        self.Key = Key
    
    def ERROR(self):
        return "key already exist, key: %d" %  (self.Key)
    

# ErrRetryable suggests that client may restart the txn. e.g. write conflict.
class ErrRetryable(BaseError):
    def ERROR(self):
        return "retryable: %s" % self.msg


# ErrAbort means something is wrong and client should abort the txn.
class ErrAbort(BaseError):
    def ERROR(self):
        return "abort: %s" % self.msg


# ErrAlreadyCommitted is returned specially when client tries to rollback a
# committed lock.
class ErrAlreadyCommitted(BaseError):
    def __init__(self, commitTS):
        self.commitTS= commitTS
    def ERROR(self):
        return "txn:%d already committed" % self.commitTS #unint64

    