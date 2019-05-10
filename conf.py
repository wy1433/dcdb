#!/usr/bin/env python
# -*-coding:utf-8 -*-
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = curPath[:curPath.find("dcdb")+len("dcdb")]
dataPath = os.path.abspath(rootPath + '/data')
#print curPath, rootPath, dataPath
DEBUG = False