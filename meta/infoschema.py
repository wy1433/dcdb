#!/usr/bin/env python
# -*-coding:utf-8 -*-
import os
import rocksdb
import threading
from enum import IntEnum
import unittest

import conf
import config
from model import *
# from util.codec.table import *
from store.mvcc_db import MvccDB
from mylog import logger

def EncodePrimary(db_name, key):
    '''
    @param rawkey: raw key to encode
    @param db_name: str . format: table_name.column_name.type, 
                    such as: student.id.data, student.name.idx etc.
    '''            
    return '%s.%s' % (db_name, key)


def DecodePrimary(primary):
    db_name, key = primary.rsplit(".", 1)
    return db_name, key


class AssocCounter(rocksdb.interfaces.AssociativeMergeOperator):
    def merge(self, key, existing_value, value):
        if existing_value:
            s = int(existing_value) + int(value)
            return (True, str(s).encode('ascii'))
        return (True, value)

    def name(self):
        return b'AssocCounter'  
        

# DBInfo provides meta data describing a DB.
class DBInfo(object):
    def __init__(self, ID=0, Name='db', tables = None):
        self.opts = rocksdb.Options()
        self.opts.create_if_missing = True
        self.opts.merge_operator = AssocCounter()
        self.db = rocksdb.DB(conf.dataPath + "/meta/dbinfo", self.opts)
        self.__lock = threading.RLock()
         
        self.ID = ID
        self.Name = Name
        
        if tables:
            self.Tables = tables
        else:
            self.Tables = config.TABLES
        
        self.table_id_dict = dict() 
        self.table_name_dict = dict()
        self.column_id_dict = dict() 
        self.column_name_dict = dict()
        self.mvccdb_id_dict = dict()
        self.mvccdb_name_dict = dict()
        
        self.stores = list() #: :type self.stores:list(MvccDB)
        
        for t in self.Tables: #: :type t: TableInfo
            self.table_id_dict[t.id] = t
            self.table_name_dict[t.name] = t
            for c in t.columns:#: :type c: ColumnInfo
                db_id, db_name = c.data_db_id, c.data_db_name
                store = MvccDB(db_id, db_name)
                self.mvccdb_id_dict[db_id] = store
                self.mvccdb_name_dict[db_name] = store
                self.stores.append(store)
                
                db_id, db_name = c.idx_db_id, c.idx_db_name
                store = MvccDB(db_id, db_name)
                self.mvccdb_id_dict[db_id] = store
                self.mvccdb_name_dict[db_name] = store
                self.stores.append(store)
                
                self.column_id_dict[c.db_id] = c
                self.column_name_dict[c.db_name] = c
            
    
    def GetTableInfoByID(self, table_id):
        return self.table_id_dict[table_id] if table_id in self.table_id_dict else None
    
    def GetTableInfoByName(self, table_name):
        return self.table_name_dict[table_name] if table_name in self.table_name_dict else None
    
    def GetColumnInfoByID(self, table_id, column_id):
        '''
        @rtype: ColumnInfo
        '''
        db_id = '%d.%d' % (table_id, column_id)
        if db_id in self.column_id_dict:
            c = self.column_id_dict[db_id]
        else:
            c = None
        return c

    def GetColumnInfoByName(self, table_name, column_name):
        '''
        @rtype: ColumnInfo
        '''
        db_name = '%s.%s' % (table_name, column_name)
        if db_name in self.column_name_dict:
            c = self.column_name_dict[db_name]
        else:
            c = None
        return c
        
    def GetMvccDB(self, db_name):
        if db_name in self.mvccdb_name_dict:
            db = self.mvccdb_name_dict[db_name]
        else:
            db = None
        return db
    
#     def GetMvccDBByPrimary(self, primary):
#         db_name, _ = self.DecodePrimary(primary)
#         return self.GetMvccDB(db_name)
    
    def EncodePrimary(self, db_name, key):
        '''
        @param rawkey: raw key to encode
        @param db_name: str . format: table_name.column_name.type, 
                        such as: student.id.data, student.name.idx etc.
        '''            
        return '%s.%s' % (db_name, key)
    
    
    def DecodePrimary(self, primary):
        db_name, key = primary.rsplit(".", 1)
        return db_name, key
    
    def Close(self):
        del self.db
        for store in self.stores: #: :type store:MvccDB
            store.Close()
    
    def GetRowID(self, table_id):
        '''Get current Rowkey, and set key = key +1 to MetaData'''
        if table_id not in self.table_id_dict:
            return -1
        with self.__lock:
            key = 'rowid_%d' % table_id
            self.db.merge(key, b'1')
            rowid = self.db.get(key)
            return int(rowid)
#         with self.__lock:
#             rowid = 0
#             key = 'rowid_%d' % table_id
#             try:
#                 rowid = self.db.get(key)
#                 rowid = int(rowid) + 1
#             except Exception as e:
#                 print e
#                 
#             try:
#                 self.db.put(key, rowid)
#             except Exception as e:
#                 print e
#             return key
        
    def ResetRowID(self, table_id):
        with self.__lock:
            key = 'rowid_%d' % table_id
            self.db.delete(key)

        
## for unittest
class TestInfoSchema(unittest.TestCase):
    Student = TableInfo(1, 'student', [
        ColumnInfo(0, 'row_id',   FieldType.INT,  IndexType.UNIQUE),
        ColumnInfo(1, 'id',       FieldType.INT,  IndexType.UNIQUE),
        ColumnInfo(2, 'name',     FieldType.STR,  IndexType.NORMAL),
        ColumnInfo(3, 'age',      FieldType.INT,  IndexType.NORMAL),
        ])
    
    Test = TableInfo(2, 'test', [
        ColumnInfo(0, 'row_id',   FieldType.INT,  IndexType.UNIQUE),
        ColumnInfo(1, 'id',       FieldType.INT,  IndexType.UNIQUE),
        ColumnInfo(2, 'course',   FieldType.STR,  IndexType.NORMAL),
        ColumnInfo(3, 'score',    FieldType.INT,  IndexType.NORMAL),
        ColumnInfo(4, 'comment',  FieldType.STR,  IndexType.NORMAL),
        ])
    
    TABLES = [
        Student,
        Test,
        ]
        
    
    def __init__(self, *args, **kwargs):
        super(TestInfoSchema, self).__init__(*args, **kwargs)
       
    @classmethod
    def setUpClass(cls):
        print('setUpClass')
        
    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    def setUp(self): 
        print'\n------',self._testMethodName, '-------'
        print "setUp" , 'init db ...'
        self.dbinfo = DBInfo(tables = self.TABLES)

    def tearDown(self):
        self.dbinfo.Close()
#         os.system('rm -rf %s/meta/*' % conf.dataPath)
#         os.system('rm -rf %s/store/*' % conf.dataPath)
        print 'tearDown', 'delete db ...'
        
    def test_GetTableInfoByID(self):
        t = self.dbinfo.GetTableInfoByID(1)
        self.assertEqual(t.name, 'student')
        t = self.dbinfo.GetTableInfoByID(2)
        self.assertEqual(t.name, 'test')
    
    def test_GetTableInfoByName(self):
        t = self.dbinfo.GetTableInfoByName('student')
        self.assertEqual(t.id, 1)
        t = self.dbinfo.GetTableInfoByName('test')
        self.assertEqual(t.id, 2)
    
    def test_GetColumnInfoByID(self):
        c = self.dbinfo.GetColumnInfoByID(1, 0)
        self.assertEqual(c.table_info.name, 'student')
        self.assertEqual(c.name, 'row_id')
        
        c = self.dbinfo.GetColumnInfoByID(1, 1)
        self.assertEqual(c.table_info.name, 'student')
        self.assertEqual(c.name, 'id')
        self.assertEqual(c.fieldType, FieldType.INT)
        self.assertEqual(c.indexType, IndexType.UNIQUE)
        
        c = self.dbinfo.GetColumnInfoByID(2, 2)
        self.assertEqual(c.table_info.name, 'test')
        self.assertEqual(c.name, 'course')
        self.assertEqual(c.fieldType, FieldType.STR)
        self.assertEqual(c.indexType, IndexType.NORMAL)
        
    
    def test_GetColumnInfoByName(self):
        c = self.dbinfo.GetColumnInfoByName('student', 'row_id')
        self.assertEqual(c.table_info.name, 'student')
        self.assertEqual(c.name, 'row_id')
        
        c = self.dbinfo.GetColumnInfoByName('student', 'id')
        self.assertEqual(c.table_info.name, 'student')
        self.assertEqual(c.name, 'id')
        self.assertEqual(c.fieldType, FieldType.INT)
        self.assertEqual(c.indexType, IndexType.UNIQUE)
        
        c = self.dbinfo.GetColumnInfoByName('test', 'course')
        self.assertEqual(c.table_info.name, 'test')
        self.assertEqual(c.name, 'course')
        self.assertEqual(c.fieldType, FieldType.STR)
        self.assertEqual(c.indexType, IndexType.NORMAL)
    
    def test_EncodePrimary(self):
        db_name = 'student.id.data'
        rawkey = '1'
        primary = self.dbinfo.EncodePrimary(db_name, rawkey)
        self.assertEqual(primary, 'student.id.data.1')
       
        
    def test_DecodePrimary(self):
        primary = 'student.id.data.1'
        db_name, key = self.dbinfo.DecodePrimary(primary)
        self.assertEqual(db_name, 'student.id.data')
        self.assertEqual(key, '1')
      
    def test_GetMvccDB(self):
        s = self.dbinfo.GetMvccDB('student.id.data')
        self.assertNotEqual(s, None)
        s = self.dbinfo.GetMvccDB('test.course.idx')
        self.assertNotEqual(s, None)
    
#     def test_GetMvccDBByPrimary(self):
#         primary = 'student.id.data.1'
#         s = self.dbinfo.GetMvccDBByPrimary(primary)
#         self.assertNotEqual(s, None)
#         
#         primary = 'student.name.idx.foo'
#         s = self.dbinfo.GetMvccDBByPrimary(primary)
#         self.assertNotEqual(s, None)
        
    data = set()
    
    def test_GetRowID(self):
        table_id = 1
        self.GetRowIDBySingleThread(table_id)
        self.GetRowIDByMultiThreads(table_id)
        self.dbinfo.ResetRowID(table_id)
    
    
    def GetRowIDBySingleThread(self, table_id):
        last_id = 0
        
        for _ in range(10):
            rid = self.dbinfo.GetRowID(table_id)
            self.assertLess(last_id, rid)
            last_id = rid
    
    def getRowID(self, table_id):
        rid = self.dbinfo.GetRowID(table_id)
        self.data.add(rid)
      
    def GetRowIDByMultiThreads(self, table_id):
        threads = []
        n = 10
        for _ in range(n):
            t = threading.Thread(target=self.getRowID, args=(table_id,))
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
        self.assertEqual(n, len(self.data))
    
                       
if __name__ == '__main__':
    pass
    unittest.main()

