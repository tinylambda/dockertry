docker run -itd \
--net=host \
--env-file /root/container_env \
--entrypoint "/root/utils/configengine.py" \
--privileged=true \
-v /data/docker-vm/tmp/:/tmp/ \
-v /data/docker-vm/var/log/zookeeper/:/var/log/zookeeper/  \
-v /data/docker-vm/var/lib/zookeeper:/var/lib/zookeeper/ \
-v /data/dfs:/data1/dfs \
lambda:5000/hadoop-base:x4