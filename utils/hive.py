#coding=utf-8
'''
Created on 2016年1月18日

@author: Felix
'''
from service import Service

class Hive(Service):
    
    REQUIRED_DIRS = [
        ('/var/lib/mysql', 'mysql:mysql', '755'),
    ]

    CONF_TMPL_DIR = '/root/templates/hive.conf.my_cluster'
    CONF_DIR = '/etc/hive/conf.my_cluster'
    CONF_FILES = ('hive-site.xml',)
    
    REQUIRED_ENVS = ['HIVE_MYSQL_ROOT_PASSWORD',
                     'HIVE_MYSQL_USER',
                     'HIVE_MYSQL_PASSWORD',
                     'HIVE_MYSQL_SERVER',
                     'HIVE_METASTORE_SERVER',
                     ]

    def __init__(self):
        Service.__init__(self)
    
    def get_render_env(self):
        self.check_env() # 先检查mysql需要的环境变量
        return self.ENV # 原样返回环境变量
    
    def configure(self):
        self.ensure_dirs()
        
        render_env = self.get_render_env()
        self.render(render_env) # 无论怎样都需要维持配置的一致性
        
        HIVE_MYSQL_USER = self.ENV.get('HIVE_MYSQL_USER')
        HIVE_MYSQL_PASSWORD = self.ENV.get('HIVE_MYSQL_PASSWORD')
        HIVE_METASTORE_SERVER = self.ENV.get('HIVE_METASTORE_SERVER')
        
        HIVE_MYSQL_SERVICE = self.ENV.get('HIVE_MYSQL_SERVICE', None)
        HIVE_METASTORE_SERVICE = self.ENV.get('HIVE_METASTORE_SERVICE', None)
        HIVE_SERVER2_SERVICE = self.ENV.get('HIVE_SERVER2_SERVICE', None)
        
        if HIVE_MYSQL_SERVICE is not None:
            HIVE_MYSQL_ROOT_PASSWORD = self.ENV.get('HIVE_MYSQL_ROOT_PASSWORD')
            HIVE_MYSQL_INITED = '/root/state/hivemysql_inited.log'
            if not self.pathexists(HIVE_MYSQL_INITED):
                # 如果Mysql还没有初始化过，进行一次初始化操作。就是设置root密码，和设置访问权限
                self.execute_cmd('service  mysqld restart') # 启动mysql服务
                self.execute_cmd('mysqladmin -uroot password "%s"' % HIVE_MYSQL_ROOT_PASSWORD) # 设置root密码
                self.execute_cmd('''echo "CREATE DATABASE metastore;" | mysql -uroot -p"%s"''' % HIVE_MYSQL_ROOT_PASSWORD) # 创建mysql中元数据库
                self.execute_cmd('''echo -e "USE metastore;\nSOURCE /usr/lib/hive/scripts/metastore/upgrade/mysql/hive-schema-1.1.0.mysql.sql" | mysql -uroot -p"%s"''' % HIVE_MYSQL_ROOT_PASSWORD)
                self.execute_cmd('''echo "CREATE USER '%s'@'%s' IDENTIFIED BY '%s'" | mysql -uroot -p"%s"''' % (HIVE_MYSQL_USER, HIVE_METASTORE_SERVER, HIVE_MYSQL_PASSWORD, HIVE_MYSQL_ROOT_PASSWORD))
                with open(HIVE_MYSQL_INITED, 'w') as hive_mysql_inited:
                    hive_mysql_inited.write(self.get_now())
            else:
                self.execute_cmd('service  mysqld restart') # 直接启动mysql服务
        
        if HIVE_METASTORE_SERVICE is not None:
            self.execute_cmd('service hive-metastore restart')
        
        if HIVE_SERVER2_SERVICE is not None:
            self.execute_cmd('service hive-server2 restart')
        