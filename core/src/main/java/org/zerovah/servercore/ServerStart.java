package org.zerovah.servercore;

import org.zerovah.servercore.akka.AkkaPathContext;
import org.zerovah.servercore.cluster.ClusterContext;
import org.zerovah.servercore.cluster.InterconnectionStrategy;
import org.zerovah.servercore.cluster.NodeHandler;
import org.zerovah.servercore.cluster.ProcessingCenter;
import org.zerovah.servercore.cluster.base.NodeConnectedFuture;
import org.zerovah.servercore.cluster.base.NodeMainType;
import org.zerovah.servercore.cluster.leader.NodeLeaderContext;
import org.zerovah.servercore.initialization.AbstractInitializer;
import org.zerovah.servercore.initialization.NodeInitializer;
import org.zerovah.servercore.util.ClassContainer;
import org.zerovah.servercore.util.PropertiesUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.zerovah.servercore.cluster.base.Clusters.*;

/**
 * 服务启动入口
 */
public class ServerStart {

	private static final Logger LOGGER = LogManager.getLogger(ServerStart.class);
	
	public static void main(String[] args) {
		try {
			LOGGER.info("操作系统信息: os.name={}, os.arch={}, os.version={}",
					System.getProperty("os.name"),
					System.getProperty("os.arch"),
					System.getProperty("os.version"));

            long start = System.currentTimeMillis();

			PropertiesUtil.loadPropertiesToSystem("node.properties");
			String scanPackage = System.getProperty("node.scan.package");
			int localNodeType = Integer.parseInt(System.getProperty("node.type"));

			ClassContainer clsContainer = new ClassContainer(scanPackage);
			AkkaPathContext akkaCtx = new AkkaPathContext();

			List<NodeInitializer> initializers = findNodeInitializers(localNodeType, clsContainer);

			LOGGER.info("节点[{}]启动初始化, 初始化执行对象:{}", localNodeType, initializers);
			ClusterContext clusterCtx = initBeforeConnectCluster(initializers, akkaCtx, clsContainer);

			Map<String, Object> mainParams = NodeInitializer.MAIN_THREAD_PARAMS.get();
			List<NodeHandler> handlers = (List) mainParams.get(NodeInitializer.NODE_HANDLERS);
			ProcessingCenter center;
			if (handlers == null) {
				center = newProcessingCenter(akkaCtx, clsContainer, localNodeType);
			} else {
				center = newProcessingCenter(akkaCtx, handlers, localNodeType);
			}
			NodeConnectedFuture nodeFuture = connectMaster(clusterCtx, localNodeType, center);

			LOGGER.info("第一阶段初始化完成, 耗时:{}毫秒", (System.currentTimeMillis() - start));

			InterconnectionStrategy strategy = clusterCtx.getInterconnectionStrategy(localNodeType);
			if (strategy.isConnectDbProxy()) {
				processingAfterConnectedDbProxyNode(initializers, clusterCtx, clsContainer);
			} else {
				processingAfterConnectedBusinessNode(initializers, clusterCtx, clsContainer);
			}

		} catch (Exception e) {
			LOGGER.error("节点服务启动过程出现异常, 启动失败", e);
		}
	}

	static List<NodeInitializer> findNodeInitializers(int localNodeType, ClassContainer clsStorage) throws Exception {
		ArrayList<NodeInitializer> initializers = new ArrayList<>(2);
		NodeLeaderContext leaderCtx = NodeLeaderContext.create(localNodeType); // 创建节点主从上下文
		Collection<Class<?>> classes = clsStorage.getClassByType(NodeInitializer.class);
		for (Class<?> cls : classes) {
			NodeInitializer initializer = (NodeInitializer) cls.newInstance();
			if (initializer.nodeType() == localNodeType) {
				initializers.add(initializer);
				initializer.initSelfNodeLeaderCtx(leaderCtx);
			}
		}
		return initializers;
	}

	static ClusterContext initBeforeConnectCluster(List<NodeInitializer> initializers, AkkaPathContext akkaCtx,
												   ClassContainer clsStorage) throws Exception {
		if (initializers.isEmpty()) {
			throw new NullPointerException("找不到节点初始化器NodeInitializer的实现类型, 请检查逻辑");
		}
		Map<Integer, InterconnectionStrategy> strategyMap = new HashMap<>();
		// ClusterContext由内部创建, 以后不再提供外部创建接口
		Map<String, Object> mainParams = NodeInitializer.MAIN_THREAD_PARAMS.get();
		Map<Integer, InterconnectionStrategy> nodeStrategys = (Map) mainParams.getOrDefault(NodeInitializer.CONNECT_STRATEGY, strategyMap);
		ClusterContext clusterCtx = ClusterContext.create(nodeStrategys);
		clusterCtx.initThreadGroup(newEventLoopGroup(NODE_CLIENT_IOTHREAD_NAME));
		// 创建ClusterContext

		for (NodeInitializer nodeInitializer : initializers) {
			if (nodeInitializer instanceof AbstractInitializer) {
				AbstractInitializer actualInitializer = (AbstractInitializer) nodeInitializer;
				actualInitializer.getMyNodeLeaderCtx().init(clusterCtx);
			}
		}
		return clusterCtx;
	}

	static void processingAfterConnectedDbProxyNode(List<NodeInitializer> initializers, ClusterContext clusterCtx,
													ClassContainer clsContainer) {
		long start = System.currentTimeMillis();
		NodeConnectedFuture nodeFuture = clusterCtx.getNodeConnectedFuture();
		AtomicInteger executedCount = new AtomicInteger();
		for (NodeInitializer nodeInitializer : initializers) {
			nodeInitializer.holdConnectedFuture(nodeFuture); // 持有异步回调对象
			nodeFuture.addFirstConnectedListener(NodeMainType.DBPROXY, data -> {
				try {
					if (nodeInitializer instanceof AbstractInitializer) {
						AbstractInitializer actualInitializer = (AbstractInitializer) nodeInitializer;
						NodeLeaderContext leaderCtx = actualInitializer.getMyNodeLeaderCtx();
						leaderCtx.addTheSameTypeNodeConnectedListener(nodeFuture);
					}
					nodeInitializer.doAfterConnectedCluster(clusterCtx, clsContainer);

					if (executedCount.incrementAndGet() == initializers.size()) {
						long elapsed = System.currentTimeMillis() - start;
						LOGGER.info("第二阶段服务启动成功, 已连接数据库代理节点, 耗时:{}毫秒, 开始清理垃圾缓存", elapsed);
						System.gc();
					}
				} catch (Exception e) {
					LOGGER.error("节点服务连接集群后处理异常", e);
				}
			});
		}
	}

	static void processingAfterConnectedBusinessNode(List<NodeInitializer> initializers, ClusterContext clusterCtx,
													ClassContainer clsStorage) {
		long start = System.currentTimeMillis();
		NodeConnectedFuture nodeFuture = clusterCtx.getNodeConnectedFuture();
		AtomicInteger executedCount = new AtomicInteger();
		for (NodeInitializer nodeInitializer : initializers) {
			nodeInitializer.holdConnectedFuture(nodeFuture); // 持有异步回调对象
			nodeFuture.addFirstConnectedListener(NodeMainType.BUSINESS, data -> {
				try {
					if (nodeInitializer instanceof AbstractInitializer) {
						AbstractInitializer actualInitializer = (AbstractInitializer) nodeInitializer;
						NodeLeaderContext leaderCtx = actualInitializer.getMyNodeLeaderCtx();
						leaderCtx.addTheSameTypeNodeConnectedListener(nodeFuture);
					}
					nodeInitializer.doAfterConnectedCluster(clusterCtx, clsStorage);

					if (executedCount.incrementAndGet() == initializers.size()) {
						long elapsed = System.currentTimeMillis() - start;
						LOGGER.info("第二阶段服务启动成功, 已连接业务节点, 耗时:{}毫秒, 开始清理垃圾缓存", elapsed);
						System.gc();
					}
				} catch (Exception e) {
					LOGGER.error("节点服务连接集群后处理异常", e);
				}
			});
		}
	}


}
