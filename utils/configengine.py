#!/usr/local/bin/python
#coding=utf-8
'''
Created on 2016年1月9日

@author: Felix
'''
import time
from zookeeper import Zookeeper
from hadoop import Hadoop
from hbase import HBase
from hive import Hive

class ConfigEngine(object):
    def __init__(self):
        self.zookeeper = Zookeeper()
        self.hadoop = Hadoop()
        self.hbase = HBase()
        self.hive = Hive()
        
    def waiting(self):
        while True:
            print('Waiting...')
            time.sleep(5)
    
    def configure(self):
        self.zookeeper.configure()
        self.hadoop.configure()
        self.hbase.configure()
        self.hive.configure()
        
if __name__ == '__main__':
    configEngine = ConfigEngine()
    configEngine.configure()
    configEngine.waiting()