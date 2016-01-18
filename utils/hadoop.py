#coding=utf-8
'''
Created on 2016年1月9日

@author: Felix
'''
from service import Service

class Hadoop(Service):
    '''
    namenode, datanode, resourcemanager, nodemanager, historyserver, proxyserver ....
    '''
    REQUIRED_DIRS = [
        ("/var/log/hadoop-0.20-mapreduce", "root:hadoop", "755"), # mr 0.20的日志目录
        ("/var/log/hadoop-hdfs", "hdfs:hadoop", "755"), # hdfs的日志目录
        ("/var/log/hadoop-mapreduce", "mapred:hadoop", "755"), # mr 2日志目录
        ("/var/log/hadoop-yarn", "yarn:hadoop", "755") # yarn日志目录
    ]
    
    CONF_TMPL_DIR = '/root/templates/hadoop.conf.my_cluster'
    CONF_DIR = '/etc/hadoop/conf.my_cluster'
    CONF_FILES = ('hdfs-site.xml', 'core-site.xml', 'mapred-site.xml', 'yarn-site.xml')
    
    REQUIRED_ENVS = [                
                "FS_DEFAULTFS",
                "HA_ZOOKEEPER_QUORUM",
                "DFS_NAMESERVICES",
                "DFS_NAMENODES_HOSTS",
                "DFS_NAMENODE_RPC_PORT",
                "DFS_NAMENODE_HTTP_PORT",
                "DFS_NAMENODE_SHARED_EDITS_DIR",
                "DFS_DATANODE_MAX_TRANSFER_THREADS",
                "MAPREDUCE_JOBHISTORY_ADDRESS",
                "MAPREDUCE_JOBHISTORY_WEBAPP_ADDRESS",
                "YARN_RESOURCEMANAGER_HOSTNAME",
                "YARN_NODEMANAGER_LOCAL_DIRS",
                "YARN_NODEMANAGER_LOG_DIRS",
    ]

    def __init__(self):
        Service.__init__(self)
        
    def get_render_env(self):
        self.check_env() # 先检查必要的环境变量
        # 经过必要的环境变量验证之后，接着要计算出一些特定于服务的变量，并将其并入当前的渲染环境变量中
        class NameNode(object):pass
        namenodes = []
        DFS_NAMENODES_HOSTS = self.ENV.get('DFS_NAMENODES_HOSTS')
        DFS_NAMENODE_RPC_PORT = self.ENV.get('DFS_NAMENODE_RPC_PORT')
        DFS_NAMENODE_HTTP_PORT = self.ENV.get('DFS_NAMENODE_HTTP_PORT')
        dfs_namenodes_hosts = DFS_NAMENODES_HOSTS.split(',')
        for dfs_namenodes_host in dfs_namenodes_hosts:
            namenode = NameNode()
            namenode.host = dfs_namenodes_host
            namenode.rpc_port = DFS_NAMENODE_RPC_PORT
            namenode.http_port = DFS_NAMENODE_HTTP_PORT
            namenodes.append(namenode)
        self.ENV.update({'namenodes': namenodes})
        return self.ENV 
        
    def configure(self):
        self.ensure_dirs()
        
        # 动态渲染配置文件，首先是以系统环境变量为基础生成一个，配置上下文，这个配置每台服务器可能都有区别，所以最好是在一个单独的方法中实现
        render_env = self.get_render_env()
        self.render(render_env)
        
        # 至此，本服务相关的配置都已经渲染到服务配置所在位置，下面就要按照环境变量的设置，决定是否启动这个服务
        NAMENODE_SERVICE = self.ENV.get('NAMENODE_SERVICE', None) # lambda or gamma for example
        DATANODE_SERVICE = self.ENV.get('DATANODE_SERVICE', None) # hostname like above
        RESOURCEMANAGER_SERVICE = self.ENV.get('RESOURCEMANAGER_SERVICE', None) # like above
        NODEMANAGER_SERVICE = self.ENV.get('NODEMANAGER_SERVICE', None)
        JOBHISTORY_SERVICE = self.ENV.get('JOBHISTORY_SERVICE', None)
        JOURNALNODE_SERVICE = self.ENV.get('JOURNALNODE_SERVICE', None)
        
        # 启动JournalNode进程
        if JOURNALNODE_SERVICE is not None:
            DFS_JOURNALNODE_EDITS_DIR = self.ENV.get('DFS_JOURNALNODE_EDITS_DIR') # like /data1/dfs/jn
            DFS_JOURNALNODE_EDITS_DIR_ARR = DFS_JOURNALNODE_EDITS_DIR.split(',')
            for dfs_journalnode_edits_dir in DFS_JOURNALNODE_EDITS_DIR_ARR:
                self.makedir((dfs_journalnode_edits_dir, 'hdfs:hdfs', '755'))
            JOURNALNODE_START_CMD = 'service hadoop-hdfs-journalnode restart'
            statusoutput = self.execute_cmd(JOURNALNODE_START_CMD)
            if statusoutput[0] == 0:
                self.success(str(statusoutput))
            else:
                self.fail('Failed to format journalnode: ' + str(statusoutput))
        
        # 启动NameNode
        if NAMENODE_SERVICE is not None:
            # 首先，需要创建HDFS名称节点元数据的存储目录，并设置合适的拥有者和权限位;如果无需启动namenode服务，自然也不需要创建这些目录
            DFS_NAMENODE_NAME_DIR = self.ENV.get('DFS_NAMENODE_NAME_DIR', None) 
            DFS_NAMENODE_NAME_DIR_ARR = DFS_NAMENODE_NAME_DIR.split(',')
            for dfs_namenode_name_dir in DFS_NAMENODE_NAME_DIR_ARR:
                self.makedir((dfs_namenode_name_dir, 'hdfs:hdfs', '700'))
            
            ZKFC_FORMATED = '/root/state/zkfc_formated.log'
            ZKFC_START_CMD = 'service hadoop-hdfs-zkfc restart'
            if NAMENODE_SERVICE.upper() == 'MASTER':
                NAMENODE_FORMATED = '/root/state/namenode_formated.log'
                if not self.pathexists(NAMENODE_FORMATED): # 如果namenode未被初始化，先执行格式化操作
                    self.success('Namenode formating...')
                    NAMENODE_FORMAT_CMD = 'sudo -u hdfs hdfs namenode -format'
                    statusoutput = self.execute_cmd(NAMENODE_FORMAT_CMD)
                    if statusoutput[0] == 0: # 如果初始化操作正常，那么打上相应的已经初始化的标记
                        with open(NAMENODE_FORMATED, 'w') as namenode_formated_file:
                            namenode_formated_file.write(self.get_now())
                    else:
                        self.fail('Failed to format namenode: ' + str(statusoutput))
                
                # 现在已经能够保证名字节点被成功格式化，下面启动namenode服务进程(注意，在部署的时候，需要先在其它服务器上启动Journalnode服务进程)
                NAMENODE_START_CMD = 'service hadoop-hdfs-namenode restart'
                statusoutput = self.execute_cmd(NAMENODE_START_CMD)
                if statusoutput[0] == 0:
                    self.success(str(statusoutput))
                else:
                    self.fail('Failed to start namenode: ' + str(statusoutput))
                
                # 如果格式化Namenode正常，则启动该节点的ZKFC服务，以用来自动切换坏掉的Namenode服务
                # 首先，要判断是否已经在Zookeeper中格式化了存储结构，如果没有需要格式化一下，本操作规定只在master
                # 节点上来做，因为Zookeeper的数据是共享的
                if not self.pathexists(ZKFC_FORMATED):
                    self.success('ZKFC initializing...')
                    ZKFC_FORMAT_CMD = 'hdfs zkfc -formatZK'
                    statusoutput = self.execute_cmd(ZKFC_FORMAT_CMD)
                    if statusoutput[0] == 0: # 如果初始化操作正常，那么打上相应的已经初始化的标记
                        with open(ZKFC_FORMATED, 'w') as state_file:
                            state_file.write(self.get_now())
                    else:
                        self.fail('Failed to init ZKFC: ' + str(statusoutput)) 
                
                statusoutput = self.execute_cmd(ZKFC_START_CMD)
                if statusoutput[0] == 0:
                    self.success(str(statusoutput))
                else:
                    self.fail('Failed to start ZKFC: ' + str(statusoutput))                        
                
                # 建立相关HDFS的目录，并设置相应权限
                self.execute_cmd('sudo -u hdfs hadoop fs -mkdir /tmp')
                self.execute_cmd('sudo -u hdfs hadoop fs -chmod -R 1777 /tmp')
                self.execute_cmd('sudo -u hdfs hadoop fs -mkdir -p /user/history')
                self.execute_cmd('sudo -u hdfs hadoop fs -chmod -R 1777 /user/history')
                self.execute_cmd('sudo -u hdfs hadoop fs -chown mapred:hadoop /user/history')
                self.execute_cmd('sudo -u hdfs hadoop fs -mkdir -p /var/log/hadoop-yarn')
                self.execute_cmd('sudo -u hdfs hadoop fs -chown yarn:mapred /var/log/hadoop-yarn')
                self.execute_cmd('sudo -u hdfs hadoop fs -mkdir  /user/hive')
                self.execute_cmd('sudo -u hdfs hadoop fs -chown hive /user/hive')
                
            elif NAMENODE_SERVICE.upper() == 'STANDBY':
                self.execute_cmd('sudo -u hdfs hdfs namenode -bootstrapStandby -force') # format anyway
                self.execute_cmd('service hadoop-hdfs-namenode restart')
                statusoutput = self.execute_cmd(ZKFC_START_CMD)
                if statusoutput[0] == 0:
                    self.success(str(statusoutput))
                else:
                    self.fail('Failed to start ZKFC: ' + str(statusoutput))             
        
        if DATANODE_SERVICE is not None:
            # 在到达这步的时候，我们必须确定NameNode的相关服务都已经搞定了，这一步只需要建立相关目录、设置好拥有者和权限位之后启动服务即可
            # 此步骤可以进一步优化，比如根据挂载点来动态确定数据目录等
            DFS_DATANODE_DATA_DIR = self.ENV.get('DFS_DATANODE_DATA_DIR')
            DFS_DATANODE_DATA_DIR_ARR = DFS_DATANODE_DATA_DIR.split(',')
            for dfs_datanode_data_dir in DFS_DATANODE_DATA_DIR_ARR:
                self.makedir((dfs_datanode_data_dir, 'hdfs:hdfs', '700'))
            
            DATANODE_START_CMD = 'service hadoop-hdfs-datanode restart'
            statusoutput = self.execute_cmd(DATANODE_START_CMD)
            if statusoutput[0] == 0:
                self.success(str(statusoutput))
            else:
                self.fail('Failed to start datanode: ' + str(statusoutput))
        
        if RESOURCEMANAGER_SERVICE is not None:
            # 设置并启动YARN的资源管理节点，
            RESOURCEMANAGER_START_CMD = 'service hadoop-yarn-resourcemanager restart'
            statusoutput = self.execute_cmd(RESOURCEMANAGER_START_CMD)
            if statusoutput[0] == 0:
                self.success(str(statusoutput))
            else:
                self.fail('Failed to start resourcemanager service: ' + str(statusoutput))
        
        if NODEMANAGER_SERVICE is not None:
            # 创建nodemanager所需要的本地工作目录和日志目录，设置好相应的拥有者和权限之后，启动服务
            YARN_NODEMANAGER_LOCAL_DIRS = self.ENV.get('YARN_NODEMANAGER_LOCAL_DIRS')
            YARN_NODEMANAGER_LOCAL_DIRS_ARR = YARN_NODEMANAGER_LOCAL_DIRS.split(',')
            for yarn_nodemanager_local_dir in YARN_NODEMANAGER_LOCAL_DIRS_ARR:
                self.makedir((yarn_nodemanager_local_dir, 'yarn:yarn', '755'))
            
            YARN_NODEMANAGER_LOG_DIRS = self.ENV.get('YARN_NODEMANAGER_LOG_DIRS')
            YARN_NODEMANAGER_LOG_DIRS_ARR = YARN_NODEMANAGER_LOG_DIRS.split(',')
            for yarn_nodemanager_log_dir in YARN_NODEMANAGER_LOG_DIRS_ARR:
                self.makedir((yarn_nodemanager_log_dir, 'yarn:yarn', '755'))
            
            NODEMANAGER_START_CMD = 'service hadoop-yarn-nodemanager restart'
            statusoutput = self.execute_cmd(NODEMANAGER_START_CMD)
            if statusoutput[0] == 0:
                self.success(str(statusoutput))
            else:
                self.fail('Failed to start nodemanager service: ' + str(statusoutput))
        
        if JOBHISTORY_SERVICE is not None:
            # 启动MapReduce Jobhistory服务
            JOBHISTORY_START_CMD = 'service hadoop-mapreduce-historyserver restart'
            statusoutput = self.execute_cmd(JOBHISTORY_START_CMD)
            if statusoutput[0] == 0:
                self.success(str(statusoutput))
            else:
                self.fail('Failed to start JOBHISTORY service: ' + str(statusoutput))