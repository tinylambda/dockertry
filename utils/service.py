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
        env.update(os.environ)
        self.ENV = env # env包含了类似于控制指令的环境变量
    
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
    
    