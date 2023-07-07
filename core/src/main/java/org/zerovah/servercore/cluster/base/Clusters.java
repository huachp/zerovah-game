package org.zerovah.servercore.cluster.base;

import akka.actor.ActorRef;
import com.alibaba.fastjson2.JSON;
import org.zerovah.servercore.akka.AkkaPathContext;
import org.zerovah.servercore.cluster.*;
import org.zerovah.servercore.cluster.actor.ActorDistributor;
import org.zerovah.servercore.cluster.iohandler.MasterServerHandler;
import org.zerovah.servercore.cluster.master.MasterClient;
import org.zerovah.servercore.cluster.master.MasterContext;
import org.zerovah.servercore.cluster.master.MasterFollowerClient;
import org.zerovah.servercore.cluster.master.NodeData;
import org.zerovah.servercore.cluster.message.C2MInitNodeMsg;
import org.zerovah.servercore.cluster.message.C2MLeaderFollowerServers;
import org.zerovah.servercore.net.socket.NettySocketClient;
import org.zerovah.servercore.net.socket.NettySocketServer;
import org.zerovah.servercore.util.ClassContainer;
import org.zerovah.servercore.util.IPAddrUtil;
import org.zerovah.servercore.util.PropertiesUtil;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public final class Clusters {

    public static final String NODE_CLIENT_IOTHREAD_NAME = "CLUSTER_NODE_CONNECTOR";
    public static final String NODE_SERVER_IOTHREAD_NAME = "CLUSTER_NODE_SERVER";
    public static final String MASTER_SERVER_IOTHREAD_NAME = "MASTER_SERVER_IO";
    public static final String MASTER_FOLLOWER_IOTHREAD_NAME = "MASTER_FOLLOWER_IO";
    public static final String MASTER_IO_CLIENT_NAME = "MASTER_CLIENT_IO";
    public static final int NODE_SERVER_ACTOR_COUNT = 64;

    // FIXME: 兼容性代码, 后面的版本会删除
    private static final ThreadLocal<ClusterContext> MAIN_THREAD_BUFFER = ThreadLocal.withInitial(() -> {
        Thread thread = Thread.currentThread();
        if (thread.getId() == 1 && thread.getName().equals("main")) { // 不是主线程则无效
            ClusterContext clusterCtx = ClusterContext.create();
            clusterCtx.initThreadGroup(newEventLoopGroup(NODE_CLIENT_IOTHREAD_NAME));
            return clusterCtx;
        }
        return null;
    });

    public static boolean startNode(ProcessingCenter center, ClusterContext clusterCtx, String ioThreadName) {
        ClusterNode localNode = clusterCtx.getLocalNode();
        InetSocketAddress address = localNode.getAddress();

        String threadName = StringUtils.isBlank(ioThreadName) ? NODE_SERVER_IOTHREAD_NAME : ioThreadName;
        NettySocketServer socketServer = NettySocketServer.create(threadName, address.getPort());
        ActorDistributor distributor = center.getDistributor();
        ClusterNodeServer nodeServer = ClusterNodeServer.create(socketServer, distributor.cluster(clusterCtx));
        return nodeServer.start();
    }

    public static void connectNode(NodeData nodeData, ClusterContext clusterCtx) {
        InetAddress[] addresses = nodeData.getInetAddresses();
        int port = nodeData.getPort();
        int nodeId = nodeData.getNodeId();
        int nodeType = nodeData.getNodeType();
        NettySocketClient socketClient = NettySocketClient.create(addresses, port);
        ClusterNodeClient nodeClient = ClusterNodeClient.create(nodeId, nodeType, socketClient)
                .ioGroup(clusterCtx.getClientThreadGroup());
        nodeClient.readyToConnect(clusterCtx);
    }

    public static ProcessingCenter newProcessingCenter(AkkaPathContext akkaCtx, ClassContainer clsContainer, int nodeType) {
        int actorCount = SystemPropertyUtil.getInt("node.actor.count", 0);
        ActorDistributor distributor = ActorDistributor.create(akkaCtx.system(),
                actorCount <= 0 ? NODE_SERVER_ACTOR_COUNT : actorCount);

        Collection<Class<?>> classes = clsContainer.getClassByType(NodeHandler.class);
        ProcessingCenter center = new ProcessingCenter(distributor);
        center.load(classes, nodeType);

        akkaCtx.holdProcessingCenter(center);
        distributor.prepareFixedActors(akkaCtx);
        return center;
    }

    public static ProcessingCenter newProcessingCenter(AkkaPathContext akkaCtx, Collection<NodeHandler> handlers, int nodeType) {
        int actorCount = SystemPropertyUtil.getInt("node.actor.count", 0);
        ActorDistributor distributor = ActorDistributor.create(akkaCtx.system(),
                actorCount <= 0 ? NODE_SERVER_ACTOR_COUNT : actorCount);

        ProcessingCenter center = new ProcessingCenter(distributor);
        center.loadHandlers(handlers, nodeType);

        akkaCtx.holdProcessingCenter(center);
        distributor.prepareFixedActors(akkaCtx);
        return center;
    }

    public static void startMaster() throws Exception {
        Properties masterProperties = PropertiesUtil.getProperties("master.properties");
        StringBuilder masterServsConf = new StringBuilder();

        Map<Integer, String> masterAddrs = new HashMap<>();
        Iterator<Map.Entry<Object, Object>> it = masterProperties.entrySet().iterator();
        for ( ; it.hasNext(); ) {
            Map.Entry<Object, Object> entry = it.next();
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            if (key.contains("master.server")) {
                String[] keyElements = StringUtils.split(key, '.');
                String id = keyElements[keyElements.length - 1];
                masterAddrs.put(Integer.parseInt(id), value);

                masterServsConf.append("\n").append(id).append(":").append(value); // 整合配置
                it.remove();
            }
        }

        TreeMap<Integer, MasterFollowerClient> clients = new TreeMap<>();
        int selfMasterId = -1;
        int selfPort = -1;
        String[] ipv4Hosts = IPAddrUtil.getInternalHostsIpv4();
        if (!masterAddrs.isEmpty()) {
            EventLoopGroup eventLoopGroup = newEventLoopGroup(MASTER_FOLLOWER_IOTHREAD_NAME);
            for (Map.Entry<Integer, String> entry : masterAddrs.entrySet()) {
                Integer masterId = entry.getKey();
                String address = entry.getValue();
                if (address.contains("127.0.0.1") || address.contains("localhost")) {
                    throw new IllegalArgumentException("配置IP不允许127.0.0.1和localhost");
                }
                String[] addrArray = StringUtils.split(address, ':');
                if (addrArray[0].equalsIgnoreCase("localhost")) {
                    throw new IllegalArgumentException("配置IP不允许127.0.0.1和localhost");
                }
                if (ArrayUtils.contains(ipv4Hosts, addrArray[0])) {
                    selfMasterId = masterId;
                    selfPort = Integer.parseInt(addrArray[1]);
                    continue;
                }
                clients.put(masterId, MasterFollowerClient.create(masterId, address, eventLoopGroup));
            }
            masterProperties.put("master.servers", masterServsConf.substring(1));
        } else {
            String masterIdText = masterProperties.getProperty("master.id", "-1");
            String portText = masterProperties.getProperty("master.port", "-1");
            selfMasterId = Integer.parseInt(masterIdText);
            selfPort = Integer.parseInt(portText);
        }
        System.getProperties().putAll(masterProperties);

        MasterContext masterCtx = MasterContext.create(selfMasterId, clients);
        NettySocketServer socketServer = NettySocketServer.create(MASTER_SERVER_IOTHREAD_NAME, selfPort);
        ClusterNodeServer masterServer = ClusterNodeServer.create(socketServer);
        ActorRef messageActor = masterCtx.createServerActor(); // 只有一个
        masterServer.startAny(() -> new MasterServerHandler(messageActor));
    }

    public static NodeConnectedFuture connectMaster(ClusterContext clusterCtx, int nodeType, ProcessingCenter center) throws Exception {
        String ip = SystemPropertyUtil.get("master.connect.host");
        int port = SystemPropertyUtil.getInt("master.connect.port", -1);

        EventLoopGroup clientThreadGroup = newEventLoopGroup(MASTER_IO_CLIENT_NAME);

        NettySocketClient socketClient = NettySocketClient.create(ip, port);
        MasterClient masterClient = MasterClient.create(socketClient, clientThreadGroup);

        ActorRef masterActor = masterClient.connect(clusterCtx, center);
        String[] addresses = IPAddrUtil.getInternalHostsIpv4();
        if (addresses.length == 0) { // 注意: 如果网卡没有配置内网IP, 那同一个集群下的节点只能部署在本地
            addresses = ArrayUtils.add(addresses, "127.0.0.1");
        }

        C2MInitNodeMsg initNodeMsg = C2MInitNodeMsg.create(nodeType, addresses);
        injectConfigData(initNodeMsg); // 传入公共配置

        C2MLeaderFollowerServers reqMasterServersInfo = C2MLeaderFollowerServers.create(nodeType);
        reqMasterServersInfo.attachInitMsg(initNodeMsg);
        masterActor.tell(reqMasterServersInfo, ActorRef.noSender());

        NodeConnectedFuture future = NodeConnectedFuture.create(clusterCtx);
        clusterCtx.nodeConnectedFuture(future);
        return future;
    }

    private static void injectConfigData(C2MInitNodeMsg initNodeMsg) {
        String jsonGroupData = System.getProperty("nodes.ip.groups");
        if (StringUtils.isNotBlank(jsonGroupData)) {
            List<NodeLocatedIpGroup> nodeGroupList = JSON.parseArray(jsonGroupData, NodeLocatedIpGroup.class);
            initNodeMsg.nodeGroups(nodeGroupList);
        }
        String workerIdData = System.getProperty("worker.ids");
        if (StringUtils.isNotBlank(workerIdData)) {
            List<Integer> workerIdList = JSON.parseArray(workerIdData, Integer.class);
            initNodeMsg.workerIds(workerIdList);
        }
        System.clearProperty("nodes.ip.groups");
        System.clearProperty("worker.ids");
    }

    // new ClusterContext的传递方式有优化修改, 后面可能会删除此接口
    @Deprecated
    public static ClusterContext newClusterContext(Map<Integer, InterconnectionStrategy> strategys) {
        ClusterContext clusterCtx = MAIN_THREAD_BUFFER.get();
        clusterCtx.initNodeStrategys(strategys);
        return clusterCtx;
    }

    public static EventLoopGroup newEventLoopGroup(String threadName) {
        DefaultThreadFactory threadFactory = new DefaultThreadFactory(threadName);
        int threadCount = NettyRuntime.availableProcessors() * 2;
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(threadCount, threadFactory);
        } else {
            return new NioEventLoopGroup(threadCount, threadFactory);
        }
//        if (PlatformDependent.isWindows()) {
//            return new NioEventLoopGroup(threadCount, threadFactory);
//        } else if (PlatformDependent.isOsx()) {
//            return new NioEventLoopGroup(threadCount, threadFactory);
//        } else {
//            return new EpollEventLoopGroup(threadCount, threadFactory);
//        }
    }

    public static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    public static int toPowerOfTwo(int val) {
        return Integer.highestOneBit(val) << 1;
    }
}
