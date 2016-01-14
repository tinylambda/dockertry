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

class ConfigEngine(object):
    def __init__(self):
        self.zookeeper = Zookeeper()
        self.hadoop = Hadoop()
        self.hbase = HBase()
        
    def waiting(self):
        while True:
            time.sleep(5)
    
    def configure(self):
        self.zookeeper.configure()
        self.hadoop.configure()
        self.hbase.configure()
        
if __name__ == '__main__':
    configEngine = ConfigEngine()
    configEngine.configure()
    configEngine.waiting()