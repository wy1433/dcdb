#!/usr/bin/env python
# -*-coding:utf-8 -*-
import unittest
import rocksdb

class MyDB(object):
    def __init__(self):
        opts = rocksdb.Options()
        opts.create_if_missing = True
        self.db = rocksdb.DB('test.db', opts)
        print 'init db'
    
    def __del__(self):
        print "__del__"
    
    def Close(self):
        del self.db

def EncodeInt(n):
    return b'%020d' % n
    

if __name__ == '__main__':
    pass
    for i in range(100):
        print EncodeInt(i), EncodeInt(i+1)
        assert EncodeInt(i) < EncodeInt(i+1)
        print EncodeInt(-i) , EncodeInt(-(i+1))
#         assert EncodeInt(-i) > EncodeInt(-(i+1))
        
    

#     unittest.main()
#     pass
#     db = MyDB()
#     del db
#     db2 = MyDB()

    
