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
package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.config.server.model.SampleResult;
import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.utils.GroupKey;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5Util;
import com.alibaba.nacos.config.server.utils.RequestUtil;
import com.alibaba.nacos.config.server.utils.event.EventDispatcher.AbstractEventListener;
import com.alibaba.nacos.config.server.utils.event.EventDispatcher.Event;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.config.server.utils.LogUtil.memoryLog;
import static com.alibaba.nacos.config.server.utils.LogUtil.pullLog;

/**
 * 长轮询服务。负责处理
 *
 * @author Nacos
 */
@Service
public class LongPollingService extends AbstractEventListener {

    private static final int FIXED_POLLING_INTERVAL_MS = 10000;

    private static final int SAMPLE_PERIOD = 100;

    private static final int SAMPLE_TIMES = 3;

    private static final String TRUE_STR = "true";

    private Map<String, Long> retainIps = new ConcurrentHashMap<String, Long>();

    private static boolean isFixedPolling() {
        return SwitchService.getSwitchBoolean(SwitchService.FIXED_POLLING, false);
    }

    private static int getFixedPollingInterval() {
        return SwitchService.getSwitchInteger(SwitchService.FIXED_POLLING_INTERVAL, FIXED_POLLING_INTERVAL_MS);
    }

    public boolean isClientLongPolling(String clientIp) {
        return getClientPollingRecord(clientIp) != null;
    }

    public Map<String, String> getClientSubConfigInfo(String clientIp) {
        ClientLongPolling record = getClientPollingRecord(clientIp);

        if (record == null) {
            return Collections.<String, String>emptyMap();
        }

        return record.clientMd5Map;
    }

    public SampleResult getSubscribleInfo(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);

        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.clientMd5Map.containsKey(groupKey)) {
                lisentersGroupkeyStatus.put(clientLongPolling.ip, clientLongPolling.clientMd5Map.get(groupKey));
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    public SampleResult getSubscribleInfoByIp(String clientIp) {
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);

        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.ip.equals(clientIp)) {
                // 一个ip可能有多个监听
                if (!lisentersGroupkeyStatus.equals(clientLongPolling.clientMd5Map)) {
                    lisentersGroupkeyStatus.putAll(clientLongPolling.clientMd5Map);
                }
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }

    /**
     * 聚合采样结果中的采样ip和监听配置的信息；合并策略用后面的覆盖前面的是没有问题的
     *
     * @param sampleResults sample Results
     * @return Results
     */
    public SampleResult mergeSampleResult(List<SampleResult> sampleResults) {
        SampleResult mergeResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<String, String>(50);
        for (SampleResult sampleResult : sampleResults) {
            Map<String, String> lisentersGroupkeyStatusTmp = sampleResult.getLisentersGroupkeyStatus();
            for (Map.Entry<String, String> entry : lisentersGroupkeyStatusTmp.entrySet()) {
                lisentersGroupkeyStatus.put(entry.getKey(), entry.getValue());
            }
        }
        mergeResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return mergeResult;
    }

    public Map<String, Set<String>> collectApplicationSubscribeConfigInfos() {
        if (allSubs == null || allSubs.isEmpty()) {
            return null;
        }
        HashMap<String, Set<String>> app2Groupkeys = new HashMap<String, Set<String>>(50);
        for (ClientLongPolling clientLongPolling : allSubs) {
            if (StringUtils.isEmpty(clientLongPolling.appName) || "unknown".equalsIgnoreCase(
                clientLongPolling.appName)) {
                continue;
            }
            Set<String> appSubscribeConfigs = app2Groupkeys.get(clientLongPolling.appName);
            Set<String> clientSubscribeConfigs = clientLongPolling.clientMd5Map.keySet();
            if (appSubscribeConfigs == null) {
                appSubscribeConfigs = new HashSet<String>(clientSubscribeConfigs.size());
            }
            appSubscribeConfigs.addAll(clientSubscribeConfigs);
            app2Groupkeys.put(clientLongPolling.appName, appSubscribeConfigs);
        }

        return app2Groupkeys;
    }

    public SampleResult getCollectSubscribleInfo(String dataId, String group, String tenant) {
        List<SampleResult> sampleResultLst = new ArrayList<SampleResult>(50);
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfo(dataId, group, tenant);
            if (sampleTmp != null) {
                sampleResultLst.add(sampleTmp);
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.clientLog.error("sleep wrong", e);
                }
            }
        }

        SampleResult sampleResult = mergeSampleResult(sampleResultLst);
        return sampleResult;
    }

    public SampleResult getCollectSubscribleInfoByIp(String ip) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.setLisentersGroupkeyStatus(new HashMap<String, String>(50));
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfoByIp(ip);
            if (sampleTmp != null) {
                if (sampleTmp.getLisentersGroupkeyStatus() != null
                    && !sampleResult.getLisentersGroupkeyStatus().equals(sampleTmp.getLisentersGroupkeyStatus())) {
                    sampleResult.getLisentersGroupkeyStatus().putAll(sampleTmp.getLisentersGroupkeyStatus());
                }
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.clientLog.error("sleep wrong", e);
                }
            }
        }
        return sampleResult;
    }

    private ClientLongPolling getClientPollingRecord(String clientIp) {
        if (allSubs == null) {
            return null;
        }

        for (ClientLongPolling clientLongPolling : allSubs) {
            HttpServletRequest request = (HttpServletRequest) clientLongPolling.asyncContext.getRequest();

            if (clientIp.equals(RequestUtil.getRemoteIp(request))) {
                return clientLongPolling;
            }
        }

        return null;
    }

    /**
     * 从方法名字上可以推测出，这个方法应该是把客户端的长轮训请求添加到某个任务中去
     * 1. 获得客户端传递过来的超时时间，并且进行本地计算，提前500ms返回响应，这就能解释为什么客户端响应超时时间是29.5+了。当然如果isFixedPolling=true的情况下，不会提前返回响应
     * 2. 根据客户端请求过来的md5和服务器端对应的group下对应内容的md5进行比较，如果不一致，则通过generateResponse将结果返回
     * 3. 如果配置文件没有发生变化，则通过scheduler.execute 启动了一个定时任务，将客户端的长轮询请求封装成一个叫 ClientLongPolling 的任务，交给 scheduler 去执行
     * @param req
     * @param rsp
     * @param clientMd5Map
     * @param probeRequestSize
     */
    public void addLongPollingClient(HttpServletRequest req, HttpServletResponse rsp, Map<String, String> clientMd5Map,
                                     int probeRequestSize) {

        String str = req.getHeader(LongPollingService.LONG_POLLING_HEADER);//获得客户端传递过来的超时时间,默认为30s
        String noHangUpFlag = req.getHeader(LongPollingService.LONG_POLLING_NO_HANG_UP_HEADER);
        String appName = req.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        String tag = req.getHeader("Vipserver-Tag");
        int delayTime = SwitchService.getSwitchInteger(SwitchService.FIXED_DELAY_TIME, 500);

        //为了避免客户端请求超时，将客户端传递过来的超时时间缩短一定时间，默认500ms，保证能在客户端请求超时前进行返回
        long timeout = Math.max(10000, Long.parseLong(str) - delayTime);//29.5s
        if (isFixedPolling()) {//如果isFixedPolling=true的情况下，不会提前返回响应
            timeout = Math.max(10000, getFixedPollingInterval());
            // do nothing but set fix polling timeout
        } else {
            long start = System.currentTimeMillis();
            //对比客户端配置的md5是否发生变化
            List<String> changedGroups = MD5Util.compareMd5(req, rsp, clientMd5Map);
            if (changedGroups.size() > 0) {
                // 通过该方法直接返回数据，结束长轮询
                generateResponse(req, rsp, changedGroups);
                LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}|{}",
                    System.currentTimeMillis() - start, "instant", RequestUtil.getRemoteIp(req), "polling",
                    clientMd5Map.size(), probeRequestSize, changedGroups.size());
                return;
            } else if (noHangUpFlag != null && noHangUpFlag.equalsIgnoreCase(TRUE_STR)) {
                LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "nohangup",
                    RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize,
                    changedGroups.size());
                return;
            }
        }
        String ip = RequestUtil.getRemoteIp(req);
        // 一定要由HTTP线程调用，否则离开后容器会立即发送响应
        //得到AsyncContext实例之后,就会先释放容器分配给请求的线程与相关资源,
        // 然后把把实例放入了一个定时任务里面;等时间到了或者有配置变更之后,调用complete()响应完成
        final AsyncContext asyncContext = req.startAsync();//开启HttpServletRequest中的asyncContext
        // AsyncContext.setTimeout()的超时时间不准，所以只能自己控制
        asyncContext.setTimeout(0L);
        //执行长轮询线程任务ClientLongPolling
        scheduler.execute(
            new ClientLongPolling(asyncContext, clientMd5Map, ip, probeRequestSize, timeout, appName, tag));
    }

    @Override
    public List<Class<? extends Event>> interest() {
        List<Class<? extends Event>> eventTypes = new ArrayList<Class<? extends Event>>();
        eventTypes.add(LocalDataChangeEvent.class);
        return eventTypes;
    }

    @Override
    public void onEvent(Event event) {
        if (isFixedPolling()) {
            // ignore
        } else {
            //如果是LocalDataChangeEvent事件,添加到DataChangeTask任务中.
            if (event instanceof LocalDataChangeEvent) {
                LocalDataChangeEvent evt = (LocalDataChangeEvent)event;
                // 当触发LocalDataChangeEvent事件的时候，会调度DataChangeTask任务。
                scheduler.execute(new DataChangeTask(evt.groupKey, evt.isBeta, evt.betaIps));
            }
        }
    }

    static public boolean isSupportLongPolling(HttpServletRequest req) {
        return null != req.getHeader(LONG_POLLING_HEADER);
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public LongPollingService() {
        // 初始化存放客户端长轮询连接的队列
        allSubs = new ConcurrentLinkedQueue<ClientLongPolling>();

        scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("com.alibaba.nacos.LongPolling");
                return t;
            }
        });
        scheduler.scheduleWithFixedDelay(new StatTask(), 0L, 10L, TimeUnit.SECONDS);
    }

    // =================

    static public final String LONG_POLLING_HEADER = "Long-Pulling-Timeout";
    static public final String LONG_POLLING_NO_HANG_UP_HEADER = "Long-Pulling-Timeout-No-Hangup";

    final ScheduledExecutorService scheduler;

    /**
     * 长轮询订阅关系
     */
    final Queue<ClientLongPolling> allSubs;

    // =================

    //数据变化的任务：有数据变化时执行该任务，遍历allSubs队列中的客户端请求
    class DataChangeTask implements Runnable {
        @Override
        public void run() {
            try {
                ConfigService.getContentBetaMd5(groupKey);
                // 遍历allSubs队列
                for (Iterator<ClientLongPolling> iter = allSubs.iterator(); iter.hasNext(); ) {
                    ClientLongPolling clientSub = iter.next();
                    // 根据当前触发事件的配置找到对应的长轮询连接
                    if (clientSub.clientMd5Map.containsKey(groupKey)) {
                        // 如果beta发布且不在beta列表直接跳过
                        if (isBeta && !betaIps.contains(clientSub.ip)) {
                            continue;
                        }

                        // 如果tag发布且不在tag列表直接跳过
                        if (StringUtils.isNotBlank(tag) && !tag.equals(clientSub.tag)) {
                            continue;
                        }

                        getRetainIps().put(clientSub.ip, System.currentTimeMillis());
                        iter.remove(); //将当前触发了事件的长轮询连接从allSubs中剔除
                        LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}|{}",
                            (System.currentTimeMillis() - changeTime),
                            "in-advance",
                            RequestUtil.getRemoteIp((HttpServletRequest)clientSub.asyncContext.getRequest()),
                            "polling",
                            clientSub.clientMd5Map.size(), clientSub.probeRequestSize, groupKey);
                        // 直接发送返回信息，并且会把延时调度任务当前的asyncTimeoutFuture结束
                        clientSub.sendResponse(Arrays.asList(groupKey));
                    }
                }
            } catch (Throwable t) {
                LogUtil.defaultLog.error("data change error:" + t.getMessage(), t.getCause());
            }
        }

        DataChangeTask(String groupKey) {
            this(groupKey, false, null);
        }

        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps) {
            this(groupKey, isBeta, betaIps, null);
        }

        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps, String tag) {
            this.groupKey = groupKey;
            this.isBeta = isBeta;
            this.betaIps = betaIps;
            this.tag = tag;
        }

        final String groupKey;
        final long changeTime = System.currentTimeMillis();
        final boolean isBeta;
        final List<String> betaIps;
        final String tag;
    }

    // =================

    class StatTask implements Runnable {
        @Override
        public void run() {
            memoryLog.info("[long-pulling] client count " + allSubs.size());
            MetricsMonitor.getLongPollingMonitor().set(allSubs.size());
        }
    }

    // =================
     //一共做两件事：

    //一个是执行一个延时调度任务(,这个任务要阻塞29.5s才能执行，因为立马执行没有任何意义，毕竟前面addLongPollingClient中已经执行过一次了。
    //如果29.5秒内发生变化怎么办？？ allSubs！！！！ )，

    // 一个是将当前长轮询连接放入allSubs队列中。
    class ClientLongPolling implements Runnable {

        @Override
        public void run() {
            //延迟调度任务
            asyncTimeoutFuture = scheduler.schedule(new Runnable() {
                //延迟29.5秒执行该任务：（从队列中删除当前长轮询连接实例，判断是否有变化，有变化则返回，无变化也返回。当前这次长轮询结束）
                @Override
                public void run() {
                    try {
                        getRetainIps().put(ClientLongPolling.this.ip, System.currentTimeMillis());
                        /**
                         * 从队列中删除当前长轮询连接实例
                         */
                        allSubs.remove(ClientLongPolling.this);

                        if (isFixedPolling()) {
                            LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}",
                                (System.currentTimeMillis() - createTime),
                                "fix", RequestUtil.getRemoteIp((HttpServletRequest)asyncContext.getRequest()),
                                "polling",
                                clientMd5Map.size(), probeRequestSize);
                            List<String> changedGroups = MD5Util.compareMd5(
                                (HttpServletRequest)asyncContext.getRequest(),
                                (HttpServletResponse)asyncContext.getResponse(), clientMd5Map);
                            if (changedGroups.size() > 0) {
                                sendResponse(changedGroups);
                            } else {
                                sendResponse(null);
                            }
                        } else {
                            LogUtil.clientLog.info("{}|{}|{}|{}|{}|{}",
                                (System.currentTimeMillis() - createTime),
                                "timeout", RequestUtil.getRemoteIp((HttpServletRequest)asyncContext.getRequest()),
                                "polling",
                                clientMd5Map.size(), probeRequestSize);
                            sendResponse(null);
                        }
                    } catch (Throwable t) {
                        LogUtil.defaultLog.error("long polling error:" + t.getMessage(), t.getCause());
                    }

                }
                // 延迟执行的时间由前面传入,timeoutTime=29.5s
            }, timeoutTime, TimeUnit.MILLISECONDS);


            // 将当前长轮询连接ClientLongPolling任务放到allSubs队列中。等待DataChangeTask数据变化任务触发执行
            allSubs.add(this);
        }

        void sendResponse(List<String> changedGroups) {
            if (null != asyncTimeoutFuture) {
                //取消延时任务
                asyncTimeoutFuture.cancel(false);
            }
            //响应请求
            generateResponse(changedGroups);
        }

        void generateResponse(List<String> changedGroups) {
            if (null == changedGroups) {
                /**
                 * 告诉容器发送HTTP响应
                 */
                asyncContext.complete();
                return;
            }

            HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();

            try {
                String respString = MD5Util.compareMd5ResultString(changedGroups);

                // 禁用缓存
                response.setHeader("Pragma", "no-cache");
                response.setDateHeader("Expires", 0);
                response.setHeader("Cache-Control", "no-cache,no-store");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().println(respString);
                asyncContext.complete();
            } catch (Exception se) {
                pullLog.error(se.toString(), se);
                asyncContext.complete();
            }
        }

        ClientLongPolling(AsyncContext ac, Map<String, String> clientMd5Map, String ip, int probeRequestSize,
                          long timeoutTime, String appName, String tag) {
            this.asyncContext = ac;
            this.clientMd5Map = clientMd5Map;
            this.probeRequestSize = probeRequestSize;
            this.createTime = System.currentTimeMillis();
            this.ip = ip;
            this.timeoutTime = timeoutTime;
            this.appName = appName;
            this.tag = tag;
        }

        // =================

        final AsyncContext asyncContext;
        final Map<String, String> clientMd5Map;
        final long createTime;
        final String ip;
        final String appName;
        final String tag;
        final int probeRequestSize;
        final long timeoutTime;

        Future<?> asyncTimeoutFuture;
    }

    void generateResponse(HttpServletRequest request, HttpServletResponse response, List<String> changedGroups) {
        if (null == changedGroups) {
            return;
        }

        try {
            String respString = MD5Util.compareMd5ResultString(changedGroups);
            // 禁用缓存
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(respString);//返回变化的结果
        } catch (Exception se) {
            pullLog.error(se.toString(), se);
        }
    }

    public Map<String, Long> getRetainIps() {
        return retainIps;
    }

    public void setRetainIps(Map<String, Long> retainIps) {
        this.retainIps = retainIps;
    }

}
