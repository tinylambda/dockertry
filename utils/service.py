#coding=utf-8
'''
Created on 2016年1月9日

@author: Felix
'''
import os, sys, commands, datetime
from jinja2 import Environment, FileSystemLoader

class Service(object):
    REQUIRED_DIRS = []
    REQUIRED_ENVS = []
    CONF_TMPL_DIR = None
    CONF_DIR = None
    CONF_FILES = ()
    
    def __init__(self):
        env = {}
        env.update(self.get_hardware_info())
        env.update(os.environ)
        self.ENV = env # env包含了类似于控制指令的环境变量
    
    def get_hardware_info(self):
        '''
        YARN_NODEMANAGER_RESOURCE_CPU_VCORES
        YARN_NODEMANAGER_RESOURCE_MEMOERY_MB
        两个参数的设定，需要根据具体容器的硬件情况个性化地配置，所以需要CPU和内存相关的信息来做依据
        '''
        LOGIC_CPU_NUM_CMD = '''cat /proc/cpuinfo | grep processor |wc -l''' # 多少线程的CPU，比如4个物理核心，支持超线程技术，则为8个逻辑核心，在系统中体现为8个CPU
        MEMORY_TOTAL_CMD = '''cat /proc/meminfo  | grep MemTotal| awk "{print $2, $3}"''' # 输出两列，第一列为数字值，第二列为单位（kb，gb）..
        CPU_INFO = self.execute_cmd(LOGIC_CPU_NUM_CMD)
        MEM_INFO = self.execute_cmd(MEMORY_TOTAL_CMD)
        CPU_NUM = int(CPU_INFO[1]) if CPU_INFO[0] == 0 else 1
        MEM_STR = MEM_INFO[1] if MEM_INFO[0] == 0 else "0 kb"
        # 将内存转化成MB单位，然后再转其它单位
        MEM_TUPLE = MEM_STR.split(" ")
        MEM_NUM = 0
        if MEM_TUPLE[1].lower() == 'kb': # 目前只处理这种情况，后来可以扩展这里的处理过程
            MEM_NUM = int(MEM_TUPLE[0]) / 1024 # MB
        
        #  默认情况下，使用容器的全部资源作为可分配的资源，如果要调整，需要根据传入的某种策略来调整
        return {'YARN_NODEMANAGER_RESOURCE_CPU_VCORES': CPU_NUM, 
                'YARN_NODEMANAGER_RESOURCE_MEMOERY_MB': MEM_NUM}
    
    def fail(self, msg):
        sys.exit(msg)
    
    def success(self, msg):
        sys.stdout.write(msg + '\n')
    
    def makedir(self, path_desc):
        pathname, owner, perm = path_desc # in format like (pathname, owner, perm)
        if not os.path.exists(pathname):
            commands.getstatusoutput('mkdir -p %s' % pathname)
        commands.getstatusoutput('chown %s %s' % (owner, pathname))
        commands.getstatusoutput('chmod %s %s' % (perm, pathname))
    
    def pathexists(self, pathname):
        return os.path.exists(pathname)
    
    def execute_cmd(self, cmd):
        self.success('Executing CMD: [%s] ...' % cmd)
        return commands.getstatusoutput(cmd)
    
    def get_now(self):
        return str(datetime.datetime.now())
    
    def check_env(self):
        # 配置需要一些必要的环境变量来渲染配置文件，如果这些环境变量不存在，则不能正确渲染配置文件，故直接退出;
        for REQUIRED_ENV in self.REQUIRED_ENVS:
            if self.ENV.get(REQUIRED_ENV, None) is None:
                self.fail('%s should be supplied' % REQUIRED_ENV)
    
    def ensure_dirs(self):
        # 首先确定所有相关的目录都是存在，并且拥有正确的拥有者和权限设置
        for REQUIRED_DIR in self.REQUIRED_DIRS:
            self.makedir(REQUIRED_DIR)
    
    def render(self, render_env):
        j2_env = Environment(loader=FileSystemLoader(self.CONF_TMPL_DIR), trim_blocks=True)
        for CONF_FILE in self.CONF_FILES:
            conf_file = os.path.join(self.CONF_DIR, CONF_FILE)
            with open(conf_file, 'w') as conf_file_obj:
                conf_file_obj.write(j2_env.get_template(CONF_FILE).render(render_env))
    
    def configure(self):
        pass
    
    