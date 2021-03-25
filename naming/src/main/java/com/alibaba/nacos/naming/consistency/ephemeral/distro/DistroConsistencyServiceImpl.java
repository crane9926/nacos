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

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.utils.ApplicationUtils;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingProxy;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 * <p>
 * Use a distro algorithm to divide data into many blocks. Each Nacos server node takes
 * responsibility for exactly one block of data. Each block of data is generated, removed
 * and synchronized by its responsible server. So every Nacos server only handles writings
 * for a subset of the total service data.
 * <p>
 * At mean time every Nacos server receives data sync of other Nacos server, so every Nacos
 * server will eventually have a complete set of data.
 *Nacos中为了实现AP，自己定制了一套Distro协议
 * 在Distro中所有的数据都是用内存进行保存，如果宕机怎么恢复？？ 看load方法！！！！
 * @author nkorange
 * @since 1.0.0
 */
@DependsOn("ProtocolManager")
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService {

	@Autowired
	private DistroMapper distroMapper;

	@Autowired
	private DataStore dataStore;

	@Autowired
	private TaskDispatcher taskDispatcher;

	@Autowired
	private Serializer serializer;

	@Autowired
	private ServerMemberManager memberManager;

	@Autowired
	private SwitchDomain switchDomain;

	@Autowired
	private GlobalConfig globalConfig;

	private boolean initialized = false;

	private volatile Notifier notifier = new Notifier();

	private LoadDataTask loadDataTask = new LoadDataTask();

	private Map<String, CopyOnWriteArrayList<RecordListener>> listeners = new ConcurrentHashMap<>();

	private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

	@PostConstruct
	public void init() {
	    //初始启动执行全量拉取
		GlobalExecutor.submit(loadDataTask);
		GlobalExecutor.submitDistroNotifyTask(notifier);
	}

	private class LoadDataTask implements Runnable {

		@Override
		public void run() {
			try {
				load();
				if (!initialized) {
					GlobalExecutor
							.submit(this, globalConfig.getLoadDataRetryDelayMillis());
				}
			}
			catch (Exception e) {
				Loggers.DISTRO.error("load data failed.", e);
			}
		}
	}

    /**
     *从别的服务器进行全量数据拉取操作，只需要执行一次即可，剩下的交由增量同步任务去完成
     * @throws Exception
     */
	public void load() throws Exception {
		if (ApplicationUtils.getStandaloneMode()) {//单例部署 无需同步
			initialized = true;
			return;
		}
		// size = 1 means only myself in the list, we need at least one another server alive:
		while (memberManager.getServerList().size() <= 1) {
			Thread.sleep(1000L);
			Loggers.DISTRO.info("waiting server list init...");
		}

		for (Map.Entry<String, Member> entry : memberManager.getServerList().entrySet()) {
			final String address = entry.getValue().getAddress();
			if (NetUtils.localServer().equals(address)) {//判断是否是自己
				continue;
			}
			if (Loggers.DISTRO.isDebugEnabled()) {
				Loggers.DISTRO.debug("sync from " + address);
			}
			// try sync data from remote server:
            // 从别的服务器进行全量数据拉取操作，只需要执行一次即可，剩下的交由增量同步任务去完成
			if (syncAllDataFromRemote(address)) {
				initialized = true;
				return;
			}
		}
	}

    /**
     * 为了防止长时间各个节点数据不一致，
     * 在DistroConsistencyService的Put方法中会对TaskDispatcher添加一个任务
     * @param key   key of data, this key should be globally unique
     * @param value value of data
     * @throws NacosException
     */
	@Override
	public void put(String key, Record value) throws NacosException {
	    //onPut在判断key是ephemeralInstanceListKey时会创建一个Datum，递增其timestamp，然后放到dataStore中。
        //并且把变化事件添加到tasks任务队列中，Notifier线程遍历tasks任务触发pushService的serviceChangeEvnet
		onPut(key, value);
		taskDispatcher.addTask(key);//把同步任务放入blockingQueue,TaskScheduler线程任务不断从queue中取出执行。
	}

    /**
     * 服务删除
     * @param key key of data
     * @throws NacosException
     */
	@Override
	public void remove(String key) throws NacosException {
		onRemove(key);
		listeners.remove(key);
	}

	@Override
	public Datum get(String key) throws NacosException {
		return dataStore.get(key);
	}

    /**
     *onPut在判断key是ephemeralInstanceListKey时会创建一个Datum，递增其timestamp，
     * 然后放到dataStore中，最后调用notifier.addTask(key, ApplyAction.CHANGE)
     *
     * 并且把变化事件添加到tasks任务队列中，Notifier线程遍历tasks任务触发pushService的serviceChangeEvnet
     * @param key
     * @param value
     */
	public void onPut(String key, Record value) {

		if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
			Datum<Instances> datum = new Datum<>();
			datum.value = (Instances) value;
			datum.key = key;
			datum.timestamp.incrementAndGet();
			dataStore.put(key, datum);
		}

		if (!listeners.containsKey(key)) {
			return;
		}

		notifier.addTask(key, ApplyAction.CHANGE);
	}

    /**
     * 将数据从dataStore中删除，
     * 并且把变化事件添加到tasks任务队列中
     * @param key
     */
	public void onRemove(String key) {

		dataStore.remove(key);

		if (!listeners.containsKey(key)) {
			return;
		}
        //把变化事件添加到tasks任务队列中
		notifier.addTask(key, ApplyAction.DELETE);
	}

    /**
     * 根据发生变化的key，主动取发出广播的服务器拉取。
     * @param checksumMap
     * @param server
     */
	public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

		if (syncChecksumTasks.containsKey(server)) {
			// Already in process of this server:
			Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
			return;
		}
        // 标记当前 Server 传来的数据正在处理
		syncChecksumTasks.put(server, "1");

		try {
            // 需要更新的 key
			List<String> toUpdateKeys = new ArrayList<>();
            // 需要删除的 Key
			List<String> toRemoveKeys = new ArrayList<>();
            // 对传来的数据进行遍历操作
			for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                // 如果传来的数据存在由本节点负责的数据，则直接退出本次数据同步操作（违反了权威server的设定要求）
				if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
					// this key should not be sent from remote server:
					Loggers.DISTRO.error("receive responsible key timestamp of " + entry
							.getKey() + " from " + server);
					// abort the procedure:
					return;
				}
                // 如果当前数据存储容器不存在这个数据，或者校验值不一样，则进行数据更新操作
				if (!dataStore.contains(entry.getKey())
						|| dataStore.get(entry.getKey()).value == null || !dataStore
						.get(entry.getKey()).value.getChecksum()
						.equals(entry.getValue())) {
					toUpdateKeys.add(entry.getKey());
				}
			}

            // 直接遍历本地数据存储容器的所有数据
			for (String key : dataStore.keys()) {
                // 如果数据不是 发送广播过来的server 负责的，则跳过。
				if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
					continue;
				}
                // 如果同步的数据不包含这个key，表明这个key是需要被删除的
				if (!checksumMap.containsKey(key)) {
					toRemoveKeys.add(key);
				}
			}

			if (Loggers.DISTRO.isDebugEnabled()) {
				Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}",
						toRemoveKeys, toUpdateKeys, server);
			}
            // 执行数据删除操作
			for (String key : toRemoveKeys) {
				onRemove(key);
			}

			if (toUpdateKeys.isEmpty()) {
				return;
			}

			try {
                // 根据需要更新的key进行数据拉取，然后对同步的数据进行操作，剩下的如同最开始的全量数据同步所做的操作
				byte[] result = NamingProxy.getData(toUpdateKeys, server);
				processData(result);
			}
			catch (Exception e) {
				Loggers.DISTRO.error("get data from " + server + " failed!", e);
			}
		}
		finally {
			// Remove this 'in process' flag:
			syncChecksumTasks.remove(server);
		}

	}

	public boolean syncAllDataFromRemote(String server) {

		try {
			byte[] data = NamingProxy.getAllData(server);
			processData(data);
			return true;
		}
		catch (Exception e) {
			Loggers.DISTRO.error("sync full data from " + server + " failed!", e);
			return false;
		}
	}

	public void processData(byte[] data) throws Exception {
		if (data.length > 0) {
			Map<String, Datum<Instances>> datumMap = serializer
					.deserializeMap(data, Instances.class);

			for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
				dataStore.put(entry.getKey(), entry.getValue());

				if (!listeners.containsKey(entry.getKey())) {
					// pretty sure the service not exist:
					if (switchDomain.isDefaultInstanceEphemeral()) {
						// create empty service
						Loggers.DISTRO.info("creating service {}", entry.getKey());
						Service service = new Service();
						String serviceName = KeyBuilder.getServiceName(entry.getKey());
						String namespaceId = KeyBuilder.getNamespace(entry.getKey());
						service.setName(serviceName);
						service.setNamespaceId(namespaceId);
						service.setGroupName(Constants.DEFAULT_GROUP);
						// now validate the service. if failed, exception will be thrown
						service.setLastModifiedMillis(System.currentTimeMillis());
						service.recalculateChecksum();
						listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).get(0).onChange(
								KeyBuilder.buildServiceMetaKey(namespaceId, serviceName),
								service);
					}
				}
			}

			for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

				if (!listeners.containsKey(entry.getKey())) {
					// Should not happen:
					Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
					continue;
				}

				try {
					for (RecordListener listener : listeners.get(entry.getKey())) {
						listener.onChange(entry.getKey(), entry.getValue().value);
					}
				}
				catch (Exception e) {
					Loggers.DISTRO
							.error("[NACOS-DISTRO] error while execute listener of key: {}",
									entry.getKey(), e);
					continue;
				}

				// Update data store if listener executed successfully:
				dataStore.put(entry.getKey(), entry.getValue());
			}
		}
	}

	@Override
	public void listen(String key, RecordListener listener) throws NacosException {
		if (!listeners.containsKey(key)) {
			listeners.put(key, new CopyOnWriteArrayList<>());
		}

		if (listeners.get(key).contains(listener)) {
			return;
		}

		listeners.get(key).add(listener);
	}

	@Override
	public void unlisten(String key, RecordListener listener) throws NacosException {
		if (!listeners.containsKey(key)) {
			return;
		}
		for (RecordListener recordListener : listeners.get(key)) {
			if (recordListener.equals(listener)) {
				listeners.get(key).remove(listener);
				break;
			}
		}
	}

	@Override
	public boolean isAvailable() {
		return isInitialized() || ServerStatus.UP.name()
				.equals(switchDomain.getOverriddenServerStatus());
	}

	public boolean isInitialized() {
		return initialized || !globalConfig.isDataWarmup();
	}

	public class Notifier implements Runnable {

		private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(
				10 * 1024);

		private BlockingQueue<Pair<String, ApplyAction>> tasks = new ArrayBlockingQueue<>(
				1024 * 1024);

		public void addTask(String datumKey, ApplyAction action) {

			if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
				return;
			}
			if (action == ApplyAction.CHANGE) {
				services.put(datumKey, StringUtils.EMPTY);
			}
			tasks.offer(Pair.with(datumKey, action));
		}

		public int getTaskSize() {
			return tasks.size();
		}

		@Override
		public void run() {
			Loggers.DISTRO.info("distro notifier started");

			for ( ; ; ) {
				try {
					Pair<String, ApplyAction> pair = tasks.take();
                    handle(pair);
				}
				catch (Throwable e) {
					Loggers.DISTRO
							.error("[NACOS-DISTRO] Error while handling notifying task",
									e);
				}
			}
		}

		private void handle(Pair<String, ApplyAction> pair) {
			try {
				String datumKey = pair.getValue0();
				ApplyAction action = pair.getValue1();

				services.remove(datumKey);

				int count = 0;

				if (!listeners.containsKey(datumKey)) {
					return;
				}

				for (RecordListener listener : listeners.get(datumKey)) {

					count++;

					try {
						if (action == ApplyAction.CHANGE) {
						    //通过service类触发pushService的serviceChange事件
							listener.onChange(datumKey, dataStore.get(datumKey).value);
							continue;
						}

						if (action == ApplyAction.DELETE) {
							listener.onDelete(datumKey);
							continue;
						}
					}
					catch (Throwable e) {
						Loggers.DISTRO
								.error("[NACOS-DISTRO] error while notifying listener of key: {}",
										datumKey, e);
					}
				}

				if (Loggers.DISTRO.isDebugEnabled()) {
					Loggers.DISTRO
							.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
									datumKey, count, action.name());
				}
			}
			catch (Throwable e) {
				Loggers.DISTRO
						.error("[NACOS-DISTRO] Error while handling notifying task", e);
			}
		}
	}
}
