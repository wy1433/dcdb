#!/usr/bin/env python
# -*-coding:utf-8 -*-
from enum import IntEnum

class Retriever():
    '''Retriever is the interface wraps the basic Get and Seek methods.'''
    
    def Get(self, k):
        '''
       # Get gets the value for key k from kv store.
       # If corresponding kv pair does not exist, it returns nil and ErrNotExist.
        Get(k Key) ([]byte, error)
        '''
        pass
    
    def Iter(self, k, upperBound):
        '''
       # Iter creates an Iterator positioned on the first entry that k <= entry's key.
       # If such entry is not found, it returns an invalid Iterator with no error.
       # It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
       # The Iterator must be Closed after use.
        Iter(k Key, upperBound Key) (Iterator, error)
        '''
        pass
    
    def IterReverse(self, k, lowerBound):
        '''
       # IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
       # The returned iterator will iterate from greater key to smaller key.
       # If k is nil, the returned iterator will be positioned at the last key.
       # Do Add lower bound limit
        IterReverse(k Key) (Iterator, error)
        '''
        pass
    
    
class Mutator():
    ''' Mutator is the interface wraps the basic Set and Delete methods.'''
    
    def Set(self, k, v):
        '''
       # Set sets the value for key k as v into kv store.
       # v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
        Set(k Key, v []byte) error
        '''
        pass
    
    def Delete(self, k):
        '''
       # Delete removes the entry for key k from kv store.
        Delete(k Key) error
        '''
        pass

class RetrieverMutator(Retriever, Mutator):
    '''RetrieverMutator is the interface that groups Retriever and Mutator interfaces.
    '''
    def __init__(self):
        pass

class MemBuffer(RetrieverMutator):
    '''MemBuffer is an in-memory kv collection, can be used to buffer write operations.'''
    def Size(self):
        '''
   # Size returns sum of keys and values length.
    Size() int
    '''
        pass
    def Len(self):
        '''
   # Len returns the number of entries in the DB.
    Len() int
    '''
        
    def Reset(self):
        '''
   # Reset cleanup the MemBuffer
    Reset()
    '''
        pass
    
    def SetCap(self):
        '''
       # SetCap sets the MemBuffer capability, to reduce memory allocations.
       # Please call it before you use the MemBuffer, otherwise it will not works.
        SetCap(cap int)
    '''
    pass

class Transaction(MemBuffer):
    '''
   # Transaction defines the interface for operations inside a Transaction.
   # This is not thread safe.
    '''
    def Commit(self, contex):
        '''
       # Commit commits the transaction operations to KV store.
        Commit(context.Context) error
        '''
        pass
    # Rollback undoes the transaction operations to KV store.
    def Rollback(self):
        pass
    
    # String implements fmt.Stringer interface.
    def String(self): # string
        pass
    # LockKeys tries to lock the entries with the keys in KV store.
    def LockKeys(self, keys): #error
        pass
    # SetOption sets an option with a value, when val is nil, uses the default
    # value of this option.
    def SetOption(self, opt, val):
        pass
    # DelOption deletes an option.
    def DelOption(self, opt):
        pass
    # IsReadOnly checks if the transaction has only performed read operations.
    def IsReadOnly(self):  #bool
        pass
    # StartTS returns the transaction start timestamp.
    def StartTS(self):  #uint64
        pass
    # Valid returns if the transaction is valid.
    # A transaction become invalid after commit or rollback.
    def Valid(self):  #bool
        pass
    # GetMemBuffer return the MemBuffer binding to this transaction.
    def GetMemBuffer(self):  #MemBuffer
        pass
    # GetSnapshot returns the snapshot of this transaction.
    def GetSnapshot(self):  #Snapshot
        pass
    # SetVars sets variables to the transaction.
    def SetVars(self, variables):
        pass


# Client is used to send request to KV layer.
class Client():
    # Send sends request to KV layer, returns a Response.
    def Send(self, ctx, req, variables): #Response
        pass

    # IsRequestTypeSupported checks if reqType and subType is supported.
    def IsRequestTypeSupported(self, reqType, subType): # bool
        pass

# ReqTypes.

class ReqType(IntEnum):
    ReqTypeSelect   = 101
    ReqTypeIndex    = 102
    ReqTypeDAG      = 103
    ReqTypeAnalyze  = 104
    ReqTypeChecksum = 105

    ReqSubTypeBasic      = 0
    ReqSubTypeDesc       = 10000
    ReqSubTypeGroupBy    = 10001
    ReqSubTypeTopN       = 10002
    ReqSubTypeSignature  = 10003
    ReqSubTypeAnalyzeIdx = 10004
    ReqSubTypeAnalyzeCol = 10005


# Request represents a kv request.
class Request():
    def __init__(self, Tp, StartTs, Data,KeyRanges,KeepOrder,Desc,Concurrency,
                 IsolationLevel,Priority,NotFillCache,SyncLog,Streaming):
        # Tp is the request type.
        self.Tp = Tp        # int64
        self.StartTs = StartTs  #uint64
        self.Data = Data      #[]byte
        self.KeyRanges=KeyRanges #[]KeyRange
        # KeepOrder is true, if the response should be returned in order.
        self.KeepOrder=KeepOrder #bool
        # Desc is true, if the request is sent in descending order.
        self.Desc=Desc #bool
        # Concurrency is 1, if it only sends the request to a single storage unit when
        # ResponseIterator.Next is called. If concurrency is greater than 1, the request will be
        # sent to multiple storage units concurrently.
        self.Concurrency=Concurrency #int
        # IsolationLevel is the isolation level, default is SI.
        self.IsolationLevel=IsolationLevel #IsoLevel
        # Priority is the priority of this KV request, its value may be PriorityNormal/PriorityLow/PriorityHigh.
        self.Priority=Priority #int
        # NotFillCache makes this request do not touch the LRU cache of the underlying storage.
        self.NotFillCache=NotFillCache #bool
        # SyncLog decides whether the WAL(write-ahead mylog) of this request should be synchronized.
        self.SyncLog=SyncLog #bool
        # Streaming indicates using streaming API for this request, result in that one Next()
        # call would not corresponds to a whole region result.
        self.Streaming=Streaming #bool


# ResultSubset represents a result subset from a single storage unit.
# TODO: Find a better interface for ResultSubset that can reuse bytes.
class ResultSubset():
    # GetData gets the data.
    def GetData(self): #[]byte
        pass
    
    # GetStartKey gets the start key.
    def GetStartKey(self): #Key
        pass
    
    # GetExecDetails gets the detail information.
    def GetExecDetails(self): #*execdetails.ExecDetails
        pass


# Response represents the response returned from KV layer.
class Response():
    # Next returns a resultSubset from a single storage unit.
    # When full result set is returned, nil is returned.
    def Next(self, ctx): #(resultSubset ResultSubset, err error)
        pass
    
    # Close response.
    def Close(self): #error
        pass


# Snapshot defines the interface for the snapshot fetched from KV store.
class Snapshot(Retriever):
    # BatchGet gets a batch of values from snapshot.
    def BatchGet(self, keys): #(map[string][]byte, error)
        pass
    
    # SetPriority snapshot set the priority
    def SetPriority(self, priority):
        pass


# Driver is the interface that must be implemented by a KV storage.
class Driver():
    # Open returns a new Storage.
    # The path is the string for storage specific format.
    def Open(self, path): #(Storage, error)
        pass


# Storage defines the interface for storage.
# Isolation should be at least SI(SNAPSHOT ISOLATION)
class Storage(object):
    # Begin transaction
    def Begin(self):# (Transaction, error)
        pass
    
    # BeginWithStartTS begins transaction with startTS.
    def BeginWithStartTS(self, startTS):# (Transaction, error)
        pass
    
    # GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
    # if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
    def GetSnapshot(self, ver):# (Snapshot, error)
        pass
    
    # GetClient gets a client instance.
    def GetClient(self):# Client
        pass
    
    # Close store
    def Close(self):# error
        pass
    
    # UUID return a unique ID which represents a Storage.
    def UUID(self):# string
        pass
    
    # CurrentVersion returns current max committed version.
    def CurrentVersion(self):# (Version, error)
        pass
    
    # GetOracle gets a timestamp oracle client.
    def GetOracle(self):# oracle.Oracle
        pass
    
    # SupportDeleteRange gets the storage support delete range or not.
    def SupportDeleteRange(self):# (supported bool)
        pass


# FnKeyCmp is the function for iterator the keys
# type FnKeyCmp func(key Key) bool

# Iterator is the interface for a iterator on KV store.
class Iterator():
    def Valid(self):# bool
        pass
    
    def Key(self):# Key
        pass
    
    def Value(self):# []byte
        pass
    
    def Next(self):# error
        pass
    
    def Close(self):#
        pass
    
