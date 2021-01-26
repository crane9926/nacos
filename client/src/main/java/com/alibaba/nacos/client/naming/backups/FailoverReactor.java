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
package com.alibaba.nacos.client.naming.backups;

import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.cache.ConcurrentDiskUtil;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.core.HostReactor;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.utils.JacksonUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * @author nkorange
 */
public class FailoverReactor {

    private String failoverDir;

    private HostReactor hostReactor;

    /**
     * 通过init()方法，开启三个定时任务
     * 1.判断容灾开关是否打开，如果打开了就读取容灾缓存文件failoverDir中的数据，否则从服务端获取最新数据。
     * 2.每隔24小时，把内存中所有的服务数据，写一遍到磁盘中。
     * 3.每隔10s检查缓存目录是否存在，同时如果缓存里面没有值的话，主动触发一次缓存写磁盘操作。
     * @param hostReactor
     * @param cacheDir
     */
    public FailoverReactor(HostReactor hostReactor, String cacheDir) {
        this.hostReactor = hostReactor;
        this.failoverDir = cacheDir + "/failover";
        this.init();
    }

    private Map<String, ServiceInfo> serviceMap = new ConcurrentHashMap<String, ServiceInfo>();
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("com.alibaba.nacos.naming.failover");
            return thread;
        }
    });

    private Map<String, String> switchParams = new ConcurrentHashMap<String, String>();
    private static final long DAY_PERIOD_MINUTES = 24 * 60;

    public void init() {
        //每隔5秒 判断一下容灾开关是否打开。(容灾模式下，服务地址读的都是缓存)
        executorService.scheduleWithFixedDelay(new SwitchRefresher(), 0L, 5000L, TimeUnit.MILLISECONDS);

        //每隔24小时，把内存中所有的服务数据，写入到磁盘的容灾缓存文件中
        executorService.scheduleWithFixedDelay(new DiskFileWriter(), 30, DAY_PERIOD_MINUTES, TimeUnit.MINUTES);

        //每隔10秒，检查容灾缓存目录是否存在，同时如果容灾缓存文件里面没有值的话，主动触发一次缓存写磁盘的操作
        // backup file on startup if failover directory is empty.
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    File cacheDir = new File(failoverDir);

                    if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                        throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                    }

                    File[] files = cacheDir.listFiles();
                    if (files == null || files.length <= 0) {
                        new DiskFileWriter().run();
                    }
                } catch (Throwable e) {
                    NAMING_LOGGER.error("[NA] failed to backup file on startup.", e);
                }

            }
        }, 10000L, TimeUnit.MILLISECONDS);
    }

    public Date addDay(Date date, int num) {
        Calendar startDT = Calendar.getInstance();
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, num);
        return startDT.getTime();
    }

    class SwitchRefresher implements Runnable {
        long lastModifiedMillis = 0L;

        @Override
        public void run() {
            try {
                //判断是否有容灾开关，容灾开关是一个磁盘文件(../failover00-00---000-VIPSRV_FAILOVER_SWITCH-000---00-00)
                File switchFile = new File(failoverDir + UtilAndComs.FAILOVER_SWITCH);
                if (!switchFile.exists()) {
                    switchParams.put("failover-mode", "false");
                    NAMING_LOGGER.debug("failover switch is not found, " + switchFile.getName());
                    return;
                }

                long modified = switchFile.lastModified();

                if (lastModifiedMillis < modified) {
                    lastModifiedMillis = modified;
                    //读取容灾开关文件(../failover00-00---000-VIPSRV_FAILOVER_SWITCH-000---00-00)中的列表
                    String failover = ConcurrentDiskUtil.getFileContent(failoverDir + UtilAndComs.FAILOVER_SWITCH,
                        Charset.defaultCharset().toString());
                    if (!StringUtils.isEmpty(failover)) {
                        List<String> lines = Arrays.asList(failover.split(DiskCache.getLineSeparator()));

                        for (String line : lines) {
                            //判定容灾开关是否打开，1表示打开，0表示关闭。
                            String line1 = line.trim();
                            if ("1".equals(line1)) {
                                switchParams.put("failover-mode", "true");
                                NAMING_LOGGER.info("failover-mode is on");
                                new FailoverFileReader().run();
                            } else if ("0".equals(line1)) {
                                switchParams.put("failover-mode", "false");
                                NAMING_LOGGER.info("failover-mode is off");
                            }
                        }
                    } else {
                        switchParams.put("failover-mode", "false");
                    }
                }

            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to read failover switch.", e);
            }
        }
    }

    /**
     * 读取容灾缓存文件failoverDir中的值
     */
    class FailoverFileReader implements Runnable {

        @Override
        public void run() {
            Map<String, ServiceInfo> domMap = new HashMap<String, ServiceInfo>(16);

            BufferedReader reader = null;
            try {

                File cacheDir = new File(failoverDir);
                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }

                File[] files = cacheDir.listFiles();
                if (files == null) {
                    return;
                }

                for (File file : files) {
                    if (!file.isFile()) {
                        continue;
                    }

                    if (file.getName().equals(UtilAndComs.FAILOVER_SWITCH)) {
                        continue;
                    }

                    ServiceInfo dom = new ServiceInfo(file.getName());

                    try {
                        String dataString = ConcurrentDiskUtil.getFileContent(file,
                            Charset.defaultCharset().toString());
                        reader = new BufferedReader(new StringReader(dataString));

                        String json;
                        if ((json = reader.readLine()) != null) {
                            try {
                                dom = JacksonUtils.toObj(json, ServiceInfo.class);
                            } catch (Exception e) {
                                NAMING_LOGGER.error("[NA] error while parsing cached dom : " + json, e);
                            }
                        }

                    } catch (Exception e) {
                        NAMING_LOGGER.error("[NA] failed to read cache for dom: " + file.getName(), e);
                    } finally {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                        } catch (Exception e) {
                            //ignore
                        }
                    }
                    if (!CollectionUtils.isEmpty(dom.getHosts())) {
                        domMap.put(dom.getKey(), dom);
                    }
                }
            } catch (Exception e) {
                NAMING_LOGGER.error("[NA] failed to read cache file", e);
            }

            if (domMap.size() > 0) {
                serviceMap = domMap;
            }
        }
    }

    /**
     * 写入 容灾缓存文件failoverDir
     */
    class DiskFileWriter extends TimerTask {
        @Override
        public void run() {
            Map<String, ServiceInfo> map = hostReactor.getServiceInfoMap();
            for (Map.Entry<String, ServiceInfo> entry : map.entrySet()) {
                ServiceInfo serviceInfo = entry.getValue();
                if (StringUtils.equals(serviceInfo.getKey(), UtilAndComs.ALL_IPS) || StringUtils.equals(
                    serviceInfo.getName(), UtilAndComs.ENV_LIST_KEY)
                    || StringUtils.equals(serviceInfo.getName(), "00-00---000-ENV_CONFIGS-000---00-00")
                    || StringUtils.equals(serviceInfo.getName(), "vipclient.properties")
                    || StringUtils.equals(serviceInfo.getName(), "00-00---000-ALL_HOSTS-000---00-00")) {
                    continue;
                }

                DiskCache.write(serviceInfo, failoverDir);
            }
        }
    }

    public boolean isFailoverSwitch() {
        return Boolean.parseBoolean(switchParams.get("failover-mode"));
    }

    public ServiceInfo getService(String key) {
        ServiceInfo serviceInfo = serviceMap.get(key);

        if (serviceInfo == null) {
            serviceInfo = new ServiceInfo();
            serviceInfo.setName(key);
        }

        return serviceInfo;
    }
}
