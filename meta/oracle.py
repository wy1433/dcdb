#!/usr/bin/env python
# -*-coding:utf-8 -*-
import unittest
import threading
import time
from store.mvcc_db_unittest import lock

Microsecond = 1
Millisecond = 1000 * Microsecond
Second = 1000 * Millisecond
Minute = 60 * Second
Hour = 60 * Minute


class Oracle(object):
    '''
     A oracle is a timestamps can be shared by multiple threads.
    '''
    _lock = threading.RLock()

    def __init__(self):
        self.__lastTS = int(time.time() * Second)

    def GetTimestamp(self):
        with Oracle._lock:
            ts = int(time.time() * Second)
            delta = self.__lastTS - ts
            if delta >= 0:
                time.sleep(delta + 1)
                self.__lastTS += 1
            else:
                self.__lastTS = ts
                
        return self.__lastTS
    
    def IsExpired(self, lockTimestamp, ttl):
        '''
        @param lockTimestamp: Microsecond
        @param TTL: Millisecond
        '''
        with Oracle._lock:        
            return int(time.time() * Second) >= lockTimestamp + ttl * Millisecond


## for unittest
class TestOracle(unittest.TestCase):
    data = set()
    oracle = Oracle()
    
    def GetTimestampBySingleThread(self):
        last_ts = 0
        for _ in range(10):
            ts = self.oracle.GetTimestamp()
            self.assertLess(last_ts, ts)
            last_ts = ts
    
    def getTimestamp(self):
        ts = self.oracle.GetTimestamp()
        self.data.add(ts)
      
    def GetTimestampByMultiThreads(self):
        threads = []
        n = 10
        for _ in range(n):
            t = threading.Thread(target=self.getTimestamp, args=())
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
        self.assertEqual(n, len(self.data))

        
    def test_GetTimestamp(self):
        self.GetTimestampBySingleThread()
        self.GetTimestampByMultiThreads()
    
    def test_IsExpired(self):
        ttl = 1000
        lockTimestamp = self.oracle.GetTimestamp()
        ret = self.oracle.IsExpired(lockTimestamp, ttl)
        self.assertEqual(ret, False)
        time.sleep(ttl/1000)
        ret = self.oracle.IsExpired(lockTimestamp, ttl)
        self.assertEqual(ret, True)
        
        
         
                       
if __name__ == '__main__':
    pass
    unittest.main()
