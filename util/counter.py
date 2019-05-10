import unittest
import threading
import time

class Counter(object):
    '''
     A counter object that can be shared by multiple threads.
    '''
    _lock = threading.RLock()
    def __init__(self, initial_value = 0):
        self._value = initial_value

    def Incr(self, delta=1):
        '''
        Increment the counter with locking
        '''
        with Counter._lock:
            self._value += delta
            
        return self._value
    
    def GetID(self):
        return self._value
    

# ## for unittest
class TestCounter(unittest.TestCase):
    data = []
    counter = Counter()
    
    def get_id(self, seconds):
        time.sleep(seconds)
        c = self.counter.Incr()
        self.data.append(c)
        
    def test_counter(self):
        threads = []
        n = 5
        for i in range(5):
            t = threading.Thread(target=self.get_id, args=(5 - i,))
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
            
        l = range(1, n + 1)
        self.assertEqual(self.data, l)
        
      
                
if __name__ == '__main__':
    unittest.main()
