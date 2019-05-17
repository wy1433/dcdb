#!/usr/bin/env python
# -*-coding:utf-8 -*-

import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
from util.error import *
import rocksdb
from collections import OrderedDict
from meta.infoschema import EncodePrimary, DecodePrimary

nil = None

class ColumnStore(object):
    def __init__(self, col, txn_id):
        self.col = col
        self.txn_id = txn_id
        self.membuf = dict()
        self.lockKeys = set()
        self.insertKeys = set()
        self.mutations = list()
        self.primary = None
    
    def Get(self, key):
        if self.membuf.has_key(key):
            return self.membuf[key]
        else:
            return None
    
    def BatchGet(self, keys):
        '''
        @param keys: list(str)
        @return: A dict contains key/value pairs, 
            the map will not contain nonexistent keys.
        '''
        ret_dict = {}
        for key in keys:
            value = self.Get(key)
            if value:
                ret_dict[key] = value
        return ret_dict
    
    def Scan(self, start = None, end = None):
        items = list()
        for k, v in self.membuf.iteritems():
            if start and k < start:
                continue
            if end and k >= end:
                continue
            items.append((k,v))
        return items
    
    def Delete(self, key):
#         if self.membuf.has_key(key):
        if key in self.insertKeys:
            self.insertKeys.remove(key)
        self.membuf[key] = None
     
    def Set(self, key, value):
        self.membuf[key] = value
    
    def Insert(self, key, value):
        if self.membuf.has_key(key):
            val = self.membuf[key]
            if val is not None:
                return ErrKeyExists
        self.insertKeys.add(key)
        self.Set(key, value)
    
    def Lockkey(self, key):
        self.lockKeys.add(key)
        
    def Lockkeys(self, keys):
        for key in keys:
            self.lockKeys.add(key)
        
    def Mutations(self):
        return self.mutations
    
    def Keys(self):
        return [m.key for m in self.mutations]
        
    def WalkBuffer(self):
        self.mutations = list()
        keys = set()
        for k, v in self.membuf.iteritems():
            mutation = kvrpcpb.Mutation()
            mutation.key = k
            # Insert => Insert
            if k in self.insertKeys:
                mutation.op = kvrpcpb.Insert
                if v:
                    mutation.value = v
            # Set => Update/Delete
            else:
                if v :
                    mutation.op =  kvrpcpb.Put    
                    mutation.value = v
                else:
                    mutation.op = kvrpcpb.Del
                    mutation.key = k
            self.mutations.append(mutation)
            keys.add(k)
              
        for k in self.lockKeys:
            if k in keys:
                continue
            mutation = kvrpcpb.Mutation(op=kvrpcpb.Lock, key = k)
            self.mutations.append(mutation)
            keys.add(k)
        if len(self.mutations) > 0:
            self.primary = EncodePrimary(self.col, self.mutations[0].key)

   
# unionStore is an in-memory Store which contains a buffer for write and a
# snapshot for read.
class UnionStore(object):
    def __init__(self, txn_id):
        self.txn_id = txn_id
        self.cs = OrderedDict()
        self.primary = None
        
    def DB(self, col):
        '''
        @rtype: ColumnStore
        @return: ColumnStore
        '''
        if col not in self.cs:
            cs = ColumnStore(col, self.txn_id)
            self.cs[col] = cs    
        return self.cs[col]
    
    def Get(self, key, col):
        return self.DB(col).Get(key)
    
    def BatchGet(self, keys, col):
        return self.DB(col).BatchGet(keys)
    
    def Scan(self, startKey, endKey, col):
        return self.DB(col).Scan(startKey, endKey)
    
    def Delete(self, key, col):
        return self.DB(col).Delete(key)
    
    def Set(self, key, value, col):
        return self.DB(col).Set(key, value)
    
    def Insert(self, key, value, col):
        return self.DB(col).Insert(key, value)
    
    def LockKey(self, key, col):
        return self.DB(col).Lockkey(key)
    
    def LockKeys(self, keys, col):
        return self.DB(col).Lockkeys(keys)
    
    def WalkBuffer(self):
        for _, db in self.cs.iteritems(): #: :type db: ColumnStore
            db.WalkBuffer()
            if self.primary is None:
                self.primary = db.primary
    
    def GetColumnStores(self):
        return self.cs.itervalues()
    