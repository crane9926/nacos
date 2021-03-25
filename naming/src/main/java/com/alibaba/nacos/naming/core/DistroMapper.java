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
package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MemberChangeEvent;
import com.alibaba.nacos.core.cluster.MemberUtils;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.notify.NotifyCenter;
import com.alibaba.nacos.core.utils.ApplicationUtils;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author nkorange
 * 权威Server的判断器
 *
 * Distro协议的一个重要部分，根据数据进行 Hash 计算查找集群节点列表中的权威节点
 */
@Component("distroMapper")
public class DistroMapper implements MemberChangeListener {

    /**
     * List of service nodes, you must ensure that the order of healthyList is the same for all nodes
     */
    private volatile List<String> healthyList = new ArrayList<>();

    private final SwitchDomain switchDomain;

    private final ServerMemberManager memberManager;

    public DistroMapper(ServerMemberManager memberManager, SwitchDomain switchDomain) {
        this.memberManager = memberManager;
        this.switchDomain = switchDomain;
    }

    public List<String> getHealthyList() {
        return healthyList;
    }

    /**
     * init server list
     */
    @PostConstruct
    public void init() {
        NotifyCenter.registerSubscribe(this);
        this.healthyList = MemberUtils.simpleMembers(memberManager.allMembers());
    }

    /**
     * 判断该数据是否可以由本节点进行响应
     * @param cluster
     * @param instance
     * @return
     */
    public boolean responsible(Cluster cluster, Instance instance) {
        return switchDomain.isHealthCheckEnabled(cluster.getServiceName())
            && !cluster.getHealthCheckTask().isCancelled()
            && responsible(cluster.getServiceName())
            && cluster.contains(instance);
    }

    /**
     * 根据 ServiceName 进行 Hash 计算，找到对应的权威节点的索引，
     * 判断是否是本节点，是的话表明该数据可以由本节点进行处理
     * @param serviceName
     * @return
     */
    public boolean responsible(String serviceName) {
        final List<String> servers = healthyList;

        if (!switchDomain.isDistroEnabled() || ApplicationUtils.getStandaloneMode()) {
            return true;
        }

        if (CollectionUtils.isEmpty(servers)) {
            // means distro config is not ready yet
            return false;
        }

        int index = servers.indexOf(NetUtils.localServer());
        int lastIndex = servers.lastIndexOf(NetUtils.localServer());
        if (lastIndex < 0 || index < 0) {
            return true;
        }
        //hash取模
        int target = distroHash(serviceName) % servers.size();
        return target >= index && target <= lastIndex;
    }

    /**
     * 根据 ServiceName 找到权威 Server 的地址
     * @param serviceName
     * @return
     */
    public String mapSrv(String serviceName) {
        final List<String> servers = healthyList;

        if (CollectionUtils.isEmpty(servers) || !switchDomain.isDistroEnabled()) {
            return NetUtils.localServer();
        }

        try {
            return servers.get(distroHash(serviceName) % servers.size());
        } catch (Throwable e) {
            Loggers.SRV_LOG.warn("distro mapper failed, return localhost: " + NetUtils.localServer(), e);
            return NetUtils.localServer();
        }
    }

    public int distroHash(String serviceName) {
        return Math.abs(Objects.hash(serviceName) % Integer.MAX_VALUE);
    }

    @Override
    public void onEvent(MemberChangeEvent event) {
        healthyList = Collections.unmodifiableList(MemberUtils.simpleMembers(event.getMembers()));
    }

    @Override
    public boolean ignoreExpireEvent() {
        return true;
    }
}
