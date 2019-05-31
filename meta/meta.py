#!/usr/bin/env python
# -*-coding:utf-8 -*-
from oracle import Oracle
from infoschema import DBInfo

class MetaNode(object):

    def __init__(self, tables):
        self.db_info = DBInfo(tables=tables)
        self.oracle = Oracle()
                
    def GetTableInfoByID(self, table_id):
        return self.db_info.GetTableInfoByID(table_id)
    
    def GetTableInfoByName(self, table_name):
        '''
        @rtype: TableInfo
        '''
        return self.db_info.GetTableInfoByName(table_name)
    
    def GetColumnInfoByID(self, table_id, column_id):
        return self.db_info.GetColumnInfoByID(table_id, column_id)
    
    def GetColumnInfoByName(self, table_name, column_name):
        '''
        @rtype: ColumnInfo
        '''
        return self.db_info.GetColumnInfoByName(table_name, column_name)
        
    def GetRowID(self, table_id):
        return self.db_info.GetRowID(table_id)

    def GetTimestamp(self):
        return self.oracle.GetTimestamp()
    
    def IsExpired(self, lockTimestamp, TTL):
        return self.oracle.IsExpired(lockTimestamp, TTL)
    
