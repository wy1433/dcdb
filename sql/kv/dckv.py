#!/usr/bin/env python
# -*-coding:utf-8 -*-
from meta.meta import MetaNode
# from meta.infoschema import ColumnInfo
from store.mvcc_db import MvccDB
from txn import Transaction
from lock_resolver import LockResolver
from mylog import logger

# Isolation should be at least SI(SNAPSHOT ISOLATION)
class DckvStore(object):
    '''
    client for db
    '''
    def __init__(self, tables=None):
        '''
        @todo: gcWorker
        '''
        self.meta = MetaNode(tables)
        self.lockResolver = LockResolver(self)
        self.gcWorker = None
        
    # Begin transaction
    def Begin(self):# (Transaction, error)
        '''
        @return: Transaction
        '''
        txn = Transaction(self)
        logger.debug('new txn, startTS=%d', txn.startTS)
        return txn
    
    # Close store
    def Close(self):# error
        self.meta.db_info.Close()
            
    # CurrentVersion returns current max committed version.
    def GetTimestamp(self):# (Version, error)
        return self.meta.GetTimestamp()
        
    def GetMvccDB(self, col):
        '''
        @param col: db_name, format: table_name.column_name.type, such as: user.id.data, user.name.idx etc.
        @rtype: MvccDB
        '''
        return self.meta.db_info.GetMvccDB(col)
    
    def GetMeta(self):
        '''
        @rtype: MetaNode
        '''
        return self.meta