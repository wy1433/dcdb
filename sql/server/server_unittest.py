import unittest
import os
import sys
from mylog import logger

from server import *

nil = None

class ServerTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ServerTestCase, self).__init__(*args, **kwargs)
        
    @classmethod
    def setUpClass(cls):
        print('setUpClass')
        cls.server = Server()
        
    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    def setUp(self): 
        print'\n------', self._testMethodName, '-------'
        
        print "setUp..." 
             
    def tearDown(self):
        print 'tearDown'
 
    def test_insert(self):
        sql = "insert into student (id, name, age) values (1, 'foo', 10)"
        resp =  ServerTestCase.server.Run(sql)
        logger.debug(resp)
    
    def test_delete(self):
        sql = "insert into student (id, name, age) values (1, 'foo', 10)"
        resp =  ServerTestCase.server.Run(sql)
        logger.debug(resp)
        
          
if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [
        ServerTestCase('test_insert'),
        ServerTestCase('test_delete'),
    ]
    suite.addTests(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)

#     unittest.main()
