DOCKERVMDIR=/data/docker-vm
mkdir -p $DOCKVMDIR
mkdir -p $DOCKVMDIR/tmp && chmod 1777 $DOCKVMDIR/tmp
mkdir -p $DOCKVMDIR/state

docker run -itd \
--net=host \
--env-file /root/container_env \
--entrypoint "/root/dockertry/utils/configengine.py" \
--privileged=true \
-v $DOCKERVMDIR/tmp/:/tmp/ \
-v $DOCKERVMDIR/var/log/zookeeper/:/var/log/zookeeper/  \
-v $DOCKERVMDIR/var/lib/zookeeper:/var/lib/zookeeper/ \
-v $DOCKERVMDIR/var/log/hadoop-0.20-mapreduce:/var/log/hadoop-0.20-mapreduce \
-v $DOCKERVMDIR/var/log/hadoop-hdfs:/var/log/hadoop-hdfs \
-v $DOCKERVMDIR/var/log/hadoop-mapreduce:/var/log/hadoop-mapreduce \
-v $DOCKERVMDIR/var/log/hadoop-yarn:/var/log/hadoop-yarn \
-v $DOCKERVMDIR/state:/root/state \
-v /data/dfs:/data1/dfs \
lambda:5000/hadoop-base:x8