#!/usr/bin/env python
# -*-coding:utf-8 -*-
from enum import IntEnum

class FieldType(IntEnum):
    INT        = 1
    STR        = 2
    
class IndexType(IntEnum):
    UNIQUE     = 1
    NORMAL     = 2
    
# ColumnInfo provides meta data describing of a table column.
class ColumnInfo():

    def __init__(self, id, name, fieldType, indexType):
        self.id = id
        self.name = name
        self.fieldType = fieldType
        self.indexType = indexType
        self.table_info = None
        
    def DataDBID(self):
        return self.data_db_id
    
    def DataDBName(self):
        return self.data_db_name
    
    def IndexDBID(self):
        return self.idx_db_id
    
    def IndexDBName(self):
        return self.idx_db_name
        
    def SetTableInfo(self, table_info):
        '''
        @param table_info: TableInfo
        '''
        self.table_info = table_info #: :type table_info: TableInfo
        self.db_id = '%d.%d' %(table_info.id, self.id)
        self.db_name = '%s.%s' %(table_info.name, self.name)
        self.data_db_id = '%d.%d.0' % (table_info.id, self.id)
        self.data_db_name = '%s.%s.data' %(table_info.name, self.name)
        self.idx_db_id = '%d.%d.1' % (table_info.id, self.id)
        self.idx_db_name = '%s.%s.idx' %(table_info.name, self.name)
#         self.rowkey_prefix = RowKeyPrefix(self.table_info.id, self.id)
#         self.indexkey_prefix = IndexKeyPrefix(self.table_info.id, self.id)

# TableInfo provides meta data describing a DB table.
class TableInfo():
    def __init__(self, id = None, name=None, columns=None):
        self.id = id
        self.name = name
        self.curr_rowid = 0
        self.max_rowid = 0
        self.columns = columns
        for c in self.columns: #: :type c: ColumnInfo
            c.SetTableInfo(self)

