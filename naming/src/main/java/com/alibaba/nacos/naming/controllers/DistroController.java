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
package com.alibaba.nacos.naming.controllers;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DataStore;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DistroConsistencyServiceImpl;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.fasterxml.jackson.databind.JsonNode;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Restful methods for Partition protocol.
 *处理distro模式下的请求，distro协议使用的是ap模式
 * @author nkorange
 * @since 1.0.0
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + "/distro")
public class DistroController {

    @Autowired
    private Serializer serializer;

    @Autowired
    private DistroConsistencyServiceImpl consistencyService;

    @Autowired
    private DataStore dataStore;

    @Autowired
    private ServiceManager serviceManager;

    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 接受其他服务发送过来的同步数据
     * @param dataMap
     * @return
     * @throws Exception
     */
    @PutMapping("/datum")
    public ResponseEntity onSyncDatum(@RequestBody Map<String, Datum<Instances>> dataMap) throws Exception {

        if (dataMap.isEmpty()) {
            Loggers.DISTRO.error("[onSync] receive empty entity!");
            throw new NacosException(NacosException.INVALID_PARAM, "receive empty entity!");
        }

        for (Map.Entry<String, Datum<Instances>> entry : dataMap.entrySet()) {
            if (KeyBuilder.matchEphemeralInstanceListKey(entry.getKey())) {
                String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                String serviceName = KeyBuilder.getServiceName(entry.getKey());
                if (!serviceManager.containService(namespaceId, serviceName)
                    && switchDomain.isDefaultInstanceEphemeral()) {
                    serviceManager.createEmptyService(namespaceId, serviceName, true);
                }
                consistencyService.onPut(entry.getKey(), entry.getValue().value);
            }
        }
        return ResponseEntity.ok("ok");
    }

    /**
     * 接受其他服务的定时任务同步的数据
     *
     * 因为量比较大，同步过来的只是变化的key, 需要自己根据key去主动拉取数据。
     * @param source
     * @param dataMap
     * @return
     */
    @PutMapping("/checksum")
    public ResponseEntity syncChecksum(@RequestParam String source, @RequestBody Map<String, String> dataMap) {
        // 数据接收操作
        consistencyService.onReceiveChecksums(dataMap, source);
        return ResponseEntity.ok("ok");
    }

    @GetMapping("/datum")
    public ResponseEntity get(@RequestBody String body) throws Exception {

        JsonNode bodyNode = JacksonUtils.toObj(body);
        String keys = bodyNode.get("keys").asText();
        String keySplitter = ",";
        Map<String, Datum> datumMap = new HashMap<>(64);
        for (String key : keys.split(keySplitter)) {
            Datum datum = consistencyService.get(key);
            if (datum == null) {
                continue;
            }
            datumMap.put(key, datum);
        }

        String content = new String(serializer.serialize(datumMap), StandardCharsets.UTF_8);
        return ResponseEntity.ok(content);
    }

    /**
     * 获取缓存中的数据
     * @return
     */
    @GetMapping("/datums")
    public ResponseEntity getAllDatums() {
        String content = new String(serializer.serialize(dataStore.getDataMap()), StandardCharsets.UTF_8);
        return ResponseEntity.ok(content);
    }
}
