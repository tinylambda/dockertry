#coding=utf-8
'''
Created on 2016年1月9日

@author: Felix
'''
from service import Service

class HBase(Service):
    REQUIRED_DIRS = [
        ('/var/log/hbase', 'hbase:hbase', '755'),
    ]
    
    CONF_TMPL_DIR = '/root/templates/hbase.conf.my_cluster'
    CONF_DIR = '/etc/hbase/conf.my_cluster'
    CONF_FILES = ('hbase-site.xml')
    
    REQUIRED_ENVS = ['FS_DEFAULTFS',
                     'HBASE_ZOOKEEPER_QUORUM',
                     ]

    def __init__(self):
        Service.__init__(self)
    
    def get_render_env(self):
        self.check_env()
        return self.ENV
    
    def configure(self):
        self.ensure_dirs()
        
        render_env = self.get_render_env()
        self.render(render_env)
        
        HBASE_MASTER_SERVICE = self.ENV.get('HBASE_MASTER_SERVICE', None)
        HBASE_REGIONSERVER_SERVICE = self.ENV.get('HBASE_REGIONSERVER_SERVICE', None)
        
        if HBASE_REGIONSERVER_SERVICE is not None:
            self.execute_cmd('service hbase-regionserver restart')
        
        if HBASE_MASTER_SERVICE is not None:
            self.execute_cmd('sudo -u hdfs hadoop fs -mkdir  /hbase')
            self.execute_cmd('sudo -u hdfs hadoop fs -chown hbase:hbase /hbase')
            self.execute_cmd('service hbase-master restart')
