/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data replicator
 *
 * @author nkorange
 * @since 1.0.0
 */
@Component
@DependsOn("ProtocolManager")
public class DataSyncer {

    @Autowired
    private DataStore dataStore;

    @Autowired
    private GlobalConfig partitionConfig;

    @Autowired
    private Serializer serializer;

    @Autowired
    private DistroMapper distroMapper;

    @Autowired
    private ServerMemberManager memberManager;

    private Map<String, String> taskMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        // 执行定期的数据同步任务（每五秒执行一次）
        startTimedSync();
    }

    // 任务提交
    public void submit(SyncTask task, long delay) {

        // If it's a new task:
        if (task.getRetryCount() == 0) {
            // 遍历所有的任务 Key
            Iterator<String> iterator = task.getKeys().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                // 数据任务放入 Map 中，避免数据同步任务重复提交
                if (StringUtils.isNotBlank(taskMap.putIfAbsent(buildKey(key, task.getTargetServer()), key))) {
                    // associated key already exist:
                    if (Loggers.DISTRO.isDebugEnabled()) {
                        Loggers.DISTRO.debug("sync already in process, key: {}", key);
                    }
                    // 如果任务已经存在，则移除该任务的 Key
                    iterator.remove();
                }
            }
        }

        // 如果所有的任务都已经移除了，结束本次任务提交
        if (task.getKeys().isEmpty()) {
            // all keys are removed:
            return;
        }

        // 异步任务执行数据同步
        GlobalExecutor.submitDataSync(() -> {
            // 1. check the server
            if (getServers() == null || getServers().isEmpty()) {
                Loggers.SRV_LOG.warn("try to sync data but server list is empty.");
                return;
            }
          // 获取数据同步任务的实际同步数据
            List<String> keys = task.getKeys();

            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("try to sync data for this keys {}.", keys);
            }
            // 2. get the datums by keys and check the datum is empty or not
            // 通过key进行批量数据获取
            Map<String, Datum> datumMap = dataStore.batchGet(keys);
            // 如果数据已经被移除了，取消本次任务
            if (datumMap == null || datumMap.isEmpty()) {
                // clear all flags of this task:
                for (String key : keys) {
                    taskMap.remove(buildKey(key, task.getTargetServer()));
                }
                return;
            }
            // 数据序列化
            byte[] data = serializer.serialize(datumMap);

            long timestamp = System.currentTimeMillis();
            //把发生变化的数据同步到其他服务器(进行增量数据同步提交给其他节点)
            boolean success = NamingProxy.syncData(data, task.getTargetServer());
            // 如果本次数据同步任务失败，则重新创建SyncTask，设置重试的次数信息
            if (!success) {
                SyncTask syncTask = new SyncTask();
                syncTask.setKeys(task.getKeys());
                syncTask.setRetryCount(task.getRetryCount() + 1);
                syncTask.setLastExecuteTime(timestamp);
                syncTask.setTargetServer(task.getTargetServer());
                retrySync(syncTask);
            } else {
                // clear all flags of this task:
                for (String key : task.getKeys()) {
                    taskMap.remove(buildKey(key, task.getTargetServer()));
                }
            }
        }, delay);
    }

    // 任务重试
    public void retrySync(SyncTask syncTask) {
        Member member = new Member();
        member.setIp(syncTask.getTargetServer().split(":")[0]);
        member.setPort(Integer.parseInt(syncTask.getTargetServer().split(":")[1]));
        if (!getServers().contains(member)) {
            // if server is no longer in healthy server list, ignore this task:
            //fix #1665 remove existing tasks
            if (syncTask.getKeys() != null) {
                for (String key : syncTask.getKeys()) {
                    taskMap.remove(buildKey(key, syncTask.getTargetServer()));
                }
            }
            return;
        }

        // TODO may choose other retry policy.
        // 自动延迟重试任务的下次执行时间
        submit(syncTask, partitionConfig.getSyncRetryDelay());
    }

    public void startTimedSync() {
        GlobalExecutor.schedulePartitionDataTimedSync(new TimedSync());
    }

    /**
     * 定时同步数据任务
     *
     * 执行周期任务
     * 每次将自己负责的数据进行广播到其他的 Server 节点
     */
    public class TimedSync implements Runnable {

        @Override
        public void run() {

            try {

                if (Loggers.DISTRO.isDebugEnabled()) {
                    Loggers.DISTRO.debug("server list is: {}", getServers());
                }

                // send local timestamps to other servers:
                Map<String, String> keyChecksums = new HashMap<>(64);
                //遍历所有的数据，将属于自己管理的数据同步。
                for (String key : dataStore.keys()) {
                    // 如果自己不是负责此数据的权威 Server，则无权对此数据做集群间的广播通知操作
                    if (!distroMapper.responsible(KeyBuilder.getServiceName(key))) {
                        continue;
                    }

                    // 获取数据操作
                    Datum datum = dataStore.get(key);
                    if (datum == null) {
                        continue;
                    }
                    // 放入数据广播列表
                    keyChecksums.put(key, datum.value.getChecksum());
                }

                if (keyChecksums.isEmpty()) {
                    return;
                }

                if (Loggers.DISTRO.isDebugEnabled()) {
                    Loggers.DISTRO.debug("sync checksums: {}", keyChecksums);
                }
                // 对集群的所有节点（除了自己），做数据广播操作
                for (Member member : getServers()) {
                    if (NetUtils.localServer().equals(member.getAddress())) {
                        continue;
                    }
                    //同步数据到其他服务器(集群间的数据广播操作)
                    NamingProxy.syncCheckSums(keyChecksums, member.getAddress());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("timed sync task failed.", e);
            }
        }

    }

    public Collection<Member> getServers() {
        return memberManager.allMembers();
    }

    public String buildKey(String key, String targetServer) {
        return key + UtilsAndCommons.CACHE_KEY_SPLITER + targetServer;
    }
}
