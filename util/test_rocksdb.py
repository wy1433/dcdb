# -*-coding:utf-8 -*-
import json
import rocksdb
import sys
import getopt

opts = rocksdb.Options()
opts.create_if_missing = True
opts.max_open_files = 300000
opts.write_buffer_size = 67108864
opts.max_write_buffer_number = 3
opts.target_file_size_base = 67108864
opts.table_factory = rocksdb.BlockBasedTableFactory(
filter_policy=rocksdb.BloomFilterPolicy(10),
block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))
db = rocksdb.DB("test.engine", opts)


def put_db():
    db.put(b"key1", b"v1")
    db.put(b"key2", b"v2")
    # engine.put(b"key3", b"v3")
    db.put(b"key4", b"v4")
    db.put(b"key5", b"v5")
    db.put(b"key6", b"v6")
    db.put(b"key7", b"v7")
    db.put(b"key8", b"v8")

def get_range(start = None, end = None):
    keys = list()
    it = db.iterkeys()
    try:
        if start is None:
            it.seek_to_first()
        else :
            it.seek(start)
        while True:
            key = it.get()
            if end and key > end:
                break
            print key
            it.next()
            keys.append(key)
    except StopIteration as e:
        print e
    except Exception as e:
        print e
    print keys
    
class StaticPrefix(rocksdb.interfaces.SliceTransform):
    def name(self):
        return b'static'

    def transform(self, src):
        return (0, 5)

    def in_domain(self, src):
        return len(src) >= 5

    def in_range(self, dst):
        return len(dst) == 5

def test_prefix():
    opts = rocksdb.Options()
    opts.create_if_missing=True
    opts.prefix_extractor = StaticPrefix()
    
    db = rocksdb.DB('test.db', opts)
    
    db.put(b'00001.x', b'x')
    db.put(b'00001.y', b'y')
    db.put(b'00001.z', b'z')
    
    db.put(b'00002.x', b'x')
    db.put(b'00002.y', b'y')
    db.put(b'00002.z', b'z')
    
    db.put(b'00003.x', b'x')
    db.put(b'00003.y', b'y')
    db.put(b'00003.z', b'z')
    
    prefix = b'00002'
    
    it = db.iteritems()
    it.seek(prefix)
    print dict(it)
    del db
    # prints {b'00002.z': b'z', b'00002.y': b'y', b'00002.x': b'x'}
    # print dict(itertools.takewhile(lambda item: item[0].startswith(prefix), it))
    
def test_insert():
    opts = rocksdb.Options()
    opts.create_if_missing=True
    db = rocksdb.DB('test.db', opts)
    
    for i in range(10000):
        k = 'k%016x' % i
        v = 'v%016x' % i
#         print k, v
        db.put(k, v) 
    


if __name__ == '__main__':
#     test_prefix()
    test_insert()
#     start = None
#     end = None
#     opts, args = getopt.getopt(sys.argv[1:],"s:e:i")
#     for opt, arg in opts:
#         if "-s" == opt:
#             start = arg
#             print u's参数已经指定，值为:' + arg
#         elif "-e" == opt:
#             end = arg
#             print u'e参数已经指定，值为:' + arg
#         elif "-i" == opt:
#             print u'i参数已经指定, init db data.'
#             put_db()
#     get_range(start, end)
