#!/usr/bin/env python
# -*-coding:utf-8 -*-
import logging

# format='%(asctime)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d %(message)s'
format='%(levelname)s: %(asctime)s [%(filename)s:%(lineno)d|%(funcName)s] %(message)s'
# DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a ' #配置输出时间的格式，注意月份和天数不要搞乱了
DATE_FORMAT = '%H:%M:%S' #配置输出时间的格式，注意月份和天数不要搞乱了
logging.basicConfig(level=logging.DEBUG, format = format, datefmt = DATE_FORMAT)
logger = logging.getLogger("dcdb")