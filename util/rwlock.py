import unittest
import threading
import time

class RWLock(object):
    def __init__(self):
        self.__rlock = threading.Lock()
        self.__wlock = threading.Lock()
        self.__reader = 0
    
    def write_acquire(self):
        self.__wlock.acquire()
    
    def write_release(self):
        self.__wlock.release()
        
    def read_acquire(self):
        self.__rlock.acquire()
        if self.__reader == 0:
            self.__wlock.acquire()
        self.__reader += 1
        self.__rlock.release()
    
    def read_release(self):
        self.__rlock.acquire()
        self.__reader -= 1
        if self.__reader == 0:
            self.__wlock.release()
        self.__rlock.release()
    
    def Lock(self):
        return self.write_acquire()
    
    def UnLock(self):
        return self.write_release()
    
    def RLock(self):
        return self.read_acquire()
    
    def RUnLock(self):
        return self.read_release()
    
    # for test
    def write_locked(self):
        return self.__wlock.locked()
    
    def read_locked(self):
        return self.__rlock.locked()

# ## for unittest
class TestClass(unittest.TestCase):
    readdata = []
    writedata = []
    def read(self, rwlock, i, seconds):
        rwlock.read_acquire()
        time.sleep(seconds)
        self.writedata.append(i)
        rwlock.read_release()
     
    def write(self, rwlock, i, seconds):
        rwlock.write_acquire()
        time.sleep(seconds)
        self.writedata.append(i)
        rwlock.write_release()
      
    def write_nolock(self, rwlock, i, seconds):
        type(rwlock)
        time.sleep(seconds)
        self.writedata.append(i)
     
    def test_lock_status(self):
        rwlock = RWLock()
    
        self.assertEqual(rwlock.write_locked(), False)
        self.assertEqual(rwlock.read_locked(), False)
        
        # read
        rwlock.read_acquire()
        self.assertEqual(rwlock.write_locked(), True)
        self.assertEqual(rwlock.read_locked(), False)
        # read
        rwlock.read_acquire()
        self.assertEqual(rwlock.read_locked(), False)
        
        rwlock.read_release()
        self.assertEqual(rwlock.write_locked(), True)
        
        rwlock.read_release()
        self.assertEqual(rwlock.write_locked(), False)
        
        # write
        rwlock.write_acquire()
        self.assertEqual(rwlock.write_locked(), True)
        rwlock.write_release()
        self.assertEqual(rwlock.write_locked(), False)
    
    def test_lock_thread(self):
        rwlock = RWLock()

        self.writedata = []
        threads = []
        for i in range(5):
            t = threading.Thread(target=self.write_nolock, args=(rwlock, i, 5 - i))
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
            
        l = range(5)
        l.reverse()
        self.assertEqual(self.writedata, l)
        
        ## ww conflict
        n = 3
        threads = []
        self.writedata = []
        for i in range(n):
            t = threading.Thread(target=self.write, args=(rwlock, i, n - i))
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
        self.assertEqual(self.writedata, range(n))
        
        ## wr conflict
        threads = []
        self.writedata = []
        self.readdata = self.writedata
        
        t = threading.Thread(target=self.write, args=(rwlock, 0, 2))
        t.start()
        threads.append(t)   
        t = threading.Thread(target=self.read, args=(rwlock, 1, 1))
        t.start()
        threads.append(t)
        for t in threads:
            t.join()
        self.assertEqual(self.writedata, range(2))
        
        ## rr noconflict
        threads = []
        self.writedata = []
        self.readdata = self.writedata
        
        t = threading.Thread(target=self.read, args=(rwlock, 0, 2))
        t.start()
        threads.append(t)   
        t = threading.Thread(target=self.read, args=(rwlock, 1, 1))
        t.start()
        threads.append(t)
        for t in threads:
            t.join()
        self.assertEqual(self.writedata, [1,0])
                
if __name__ == '__main__':
    unittest.main()
