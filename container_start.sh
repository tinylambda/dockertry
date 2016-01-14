docker run -itd \
--net=host \
--env-file /root/container_env \
--entrypoint "/root/utils/config.py" \
--privileged=true \
-v /data/docker-vm/tmp/:/tmp/ \
-v /data/docker-vm/var/log/zookeeper/:/var/log/zookeeper/  \
-v /data/docker-vm/var/lib/zookeeper:/var/lib/zookeeper/ \
-v /data/dfs:/data1/dfs \
tinylambda/hadoop-base:x2