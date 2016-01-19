#coding=utf-8
'''
Created on 2016年1月9日

@author: Felix
'''
from service import Service

class Zookeeper(Service):
    REQUIRED_DIRS = [
        ('/var/log/zookeeper', 'zookeeper:zookeeper', '755'), # Zookeeper log directory
        ('/var/lib/zookeeper', 'zookeeper:zookeeper', '755') # Zookeeper data directory
    ]
    
    CONF_TMPL_DIR = '/root/templates/zookeeper.conf.my_cluster'
    CONF_DIR = '/etc/zookeeper/conf.my_cluster'
    CONF_FILES = ('zoo.cfg',)
    REQUIRED_ENVS = ['ZK_SERVERS']
    
    def __init__(self):
        Service.__init__(self)
        
    def get_render_env(self):
        self.check_env() # 先检查必要的环境变量
        # 经过必要的环境变量验证之后，接着要计算出一些特定于服务的变量，并将其并入当前的渲染环境变量中
        class ZK(object):pass
        ZK_SERVERS = self.ENV.get('ZK_SERVERS') # 类似于： ZK_SERVERS=a00#1,a01#2,a02#3
        ZK_SERVERS_ARR = ZK_SERVERS.split(',')
        zks = []
        for ZK_SERVER in ZK_SERVERS_ARR:
            zk = ZK()
            host, ser = ZK_SERVER.split('#')
            zk.host, zk.ser = host, ser
            zks.append(zk)
        self.ENV.update({'zks': zks})
        return self.ENV
            
    def configure(self):
        self.ensure_dirs()
        
        # 动态渲染配置文件，首先是以系统环境变量为基础生成一个配置上下文，这个配置每台服务器可能都有区别，所以最好是在一个单独的方法中实现
        render_env = self.get_render_env()
        self.render(render_env)
        
        # 至此，本服务相关的配置都已经渲染到服务配置所在位置，下面就要按照环境变量的设置，决定是否启动这个服务
        ZK_SERVICE = self.ENV.get('ZK_SERVICE', None) # a00#1
        if ZK_SERVICE is not None:
            _, ser = ZK_SERVICE.split('#')
            ZK_IS_INIT = '/root/state/zkinit.log'
            if not self.pathexists(ZK_IS_INIT): # 判断Zookeeper是否需要执行初始化服务
                self.success('Zookeeper not initialized, initialize it first...')
                ZK_INIT_CMD = 'service zookeeper-server init --force --myid=%(ser)s' % {'ser': ser}
                statusoutput = self.execute_cmd(ZK_INIT_CMD)
                if statusoutput[0] == 0: # 如果初始化操作正常，那么打上相应的已经初始化的标记
                    with open(ZK_IS_INIT, 'w') as zk_is_init_file:
                        zk_is_init_file.write(self.get_now())
                else:
                    self.fail('Failed to init zookeeper: ' + str(statusoutput))
            # 如果目录都已经设置好，环境变量也符合要求，同时初始化也正常，那下一步就要启动Zookeeper服务    
            ZK_START_CMD = 'service zookeeper-server restart'
            statusoutput = self.execute_cmd(ZK_START_CMD)
            if statusoutput[0] == 0:
                self.success(str(statusoutput))
            else:
                self.fail('Failed to start zookeeper: ' + str(statusoutput))
        
if __name__ == '__main__':
    zk = Zookeeper()
    zk.configure()
    
    
    
    