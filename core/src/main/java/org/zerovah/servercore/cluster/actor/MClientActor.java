package org.zerovah.servercore.cluster.actor;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.actor.TimerScheduler;
import akka.japi.pf.ReceiveBuilder;
import org.zerovah.servercore.cluster.*;
import org.zerovah.servercore.cluster.base.Clusters;
import org.zerovah.servercore.cluster.base.NodeMainType;
import org.zerovah.servercore.cluster.cause.NodeException;
import org.zerovah.servercore.cluster.master.MasterActorsPool.MasterReconnectFuture;
import org.zerovah.servercore.cluster.master.MasterClient;
import org.zerovah.servercore.cluster.master.MasterResult;
import org.zerovah.servercore.cluster.master.NodeData;
import org.zerovah.servercore.cluster.master.RaftState;
import org.zerovah.servercore.cluster.message.*;
import org.zerovah.servercore.net.socket.NettySocketClient;
import io.netty.channel.ChannelId;
import io.netty.channel.EventLoopGroup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master客户端Actor
 *
 * @author huachp
 */
public class MClientActor extends AbstractActorWithTimers {

    private static final Logger LOGGER = LogManager.getLogger(MClientActor.class);

    private static final int NUMBER_OF_FAILURES = 3;

    private static final int HEART_BEAT_TIMEOUTS = 3;

    private static final int GENERIC_TIMEOUT = 5000;

    private static final FaultTicker FAULT_TICKER = new FaultTicker();

    static final ContextAccessRights ctxAccess = ContextAccessRights.access();


    private MasterClient client;

    private TreeMap<Integer, MasterClient> followerClients = new TreeMap<>();

    private int standbyMasterId; // 只在Master Leader故障的时候, 才启用备用的Master, -1代表单点Master

    private ClusterContext clusterCtx;

    private ProcessingCenter center;

    private LinkedList<MasterMessage> keyMessages = new LinkedList<>(); // 关键消息

    private int heartbeatTimeouts;

    private int disconnectedCount; // 断开连接的持续次数

    private boolean prepared; // 已准备好的

    private ServCallbackHolder callbackHolder;

    private final Receive receive = ReceiveBuilder.create()
            .match(MasterMessage.class, this::doRemoteCall)
            .match(M2CLeaderFollowerServers.class, this::processMasterServersInfo)
            .match(M2CInitNodeMsg.class, this::doInitNode)
            .match(M2CNodeListMsg.class, this::connectNodes)
            .match(M2CNodeMsg.class, this::connectNode)
            .match(M2CStartFaultVote.class, this::faultVote)
            .match(M2CSwitchoverMaster.class, this::doSwitchoverMaster)
            .match(HeartbeartMsg.class, this::heartbeat)
            .match(NodeFaultMsg.class, this::judgeNodeFault)
            .match(FaultTicker.class, this::doFaultDetection)
            .matchAny(this::doOtherOperation)
            .build();

    public MClientActor(MasterClient client, ClusterContext ctx, ProcessingCenter center) {
        this.client = client;
        this.clusterCtx = ctx;
        this.center = center;
    }

    public static Props props(MasterClient client, ClusterContext ctx, ProcessingCenter center) {
        return Props.create(MClientActor.class, client, ctx, center);
    }

    // 这个方法的调用必须在初始化消息发送之后
    private String[] getLocalAddresses() {
        C2MInitNodeMsg initNodeMsg = (C2MInitNodeMsg) keyMessages.peek(); // 第一个核心消息
        return initNodeMsg.getAddresses();
    }

    @Override
    public Receive createReceive() {
        return receive;
    }

    private void doRemoteCall(MasterMessage message) {
        if (message instanceof C2MLeaderFollowerServers) {
            requireMasterServersAddrs((C2MLeaderFollowerServers) message); return;
        }
        if (!prepared) {
            if (message instanceof C2MInitiateFaultElection
                    || message instanceof C2MNodeFaultVote) {
                LOGGER.info("Master服务状态尚未准备就绪, 舍弃消息:{}", message); return;
            }
        }
        if (message instanceof AbstractMasterCallbackStub) {
            AbstractMasterCallbackStub stub = (AbstractMasterCallbackStub) message;
            ICallback callback = stub.getCallback();
            stub.injectId(callbackHolder.associateIdAndCallback(GENERIC_TIMEOUT, callback));
        }
        writeMessage(message);
    }

    private void writeMessage(MasterMessage message, ICallback callback) {
        callbackHolder.associateMsgAndCallback(message, GENERIC_TIMEOUT, callback);
        writeMessage(message);
    }

    private void writeMessage(MasterMessage message) {
        MasterClient masterClient = client; // 主
        if (standbyMasterId > 0) { // 主不可用
            masterClient = followerClients.get(standbyMasterId); // 从
        }
        masterClient.writeAndFlush(message);
    }

    private void requireMasterServersAddrs(C2MLeaderFollowerServers firstMessage) {
        writeMessage(firstMessage, ((ICallback) data -> {
            C2MInitNodeMsg initNodeMsg = firstMessage.takeInitNodeMsg();
            if (initNodeMsg != null) {
                keyMessages.addFirst(initNodeMsg);
            }
        }).exceptionally(timeout -> {
            LOGGER.info("获取Master主从地址信息关键消息超时, 请确保初始连接的Master服务可用");
            requireMasterServersAddrs(firstMessage);
        }));
    }

    private void processMasterServersInfo(M2CLeaderFollowerServers serversMsg) {
        try {
            callbackHolder.defaultSuccessCallback(C2MLeaderFollowerServers.class);

            Map<Integer, InetSocketAddress> serverInfos = serversMsg.getMasterAddrs();
            if (followerClients.size() > 0 || standbyMasterId == -1) {
                LOGGER.info("已初始化Master主从连接信息, 当前连接的Master主从数:{}, 返回的的Master主从地址数:{}",
                        followerClients.size() + 1, serverInfos.size());
                return;
            }

            if (serverInfos.size() == 1) { // 单点Master
                standbyMasterId = -1;
            }
            int connectedMasterId = serversMsg.getConnectedMasterId();
            client.keepMasterIdFromServer(connectedMasterId);
            int leaderMasterId = connectedMasterId;
            for (int elementMasterId : serverInfos.keySet()) {
                if (elementMasterId < leaderMasterId) {
                    leaderMasterId = elementMasterId;
                }
            }

            LOGGER.info("获取到Master主从地址信息:{}", serverInfos);
            EventLoopGroup clientEventLoop = client.getClient().getEventLoopGroup();
            if (leaderMasterId != client.getMasterId()) {
                InetSocketAddress masterServerAddr = serverInfos.get(leaderMasterId);
                MasterClient masterClient = connectNewMaster(leaderMasterId, masterServerAddr, clientEventLoop);

                MasterClient followerClient = client;
                client = masterClient;
                followerClients.put(followerClient.getMasterId(), followerClient);

                serverInfos.remove(leaderMasterId);
            }
            serverInfos.remove(connectedMasterId); // 移除Actor启动时连接的MasterId信息
            for (Entry<Integer, InetSocketAddress> entry : serverInfos.entrySet()) {
                int masterId = entry.getKey();
                InetSocketAddress socketAddress = entry.getValue();
                MasterClient nextClient = connectNewMaster(masterId, socketAddress, clientEventLoop);
                followerClients.put(masterId, nextClient);
            }

            C2MInitNodeMsg initNodeMsg = (C2MInitNodeMsg) keyMessages.peek(); // 第一个消息一定是初始化消息
            doRemoteCall(initNodeMsg);

        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private MasterClient connectNewMaster(int masterId, InetSocketAddress socketAddr, EventLoopGroup eventLoop) {
        String ip = socketAddr.getAddress().getHostAddress();
        int port = socketAddr.getPort();

        NettySocketClient socketClient = NettySocketClient.create(ip, port);
        MasterClient masterClient = MasterClient.create(masterId, socketClient);
        masterClient.ioGroup(eventLoop);
        masterClient.connect(self());
        return masterClient;
    }

    private void doInitNode(M2CInitNodeMsg initMsg) {
        try {
            int nodeId = initMsg.getNodeId();
            int nodeType = initMsg.getNodeType();
            int port = initMsg.getPort();
            int masterId = initMsg.getMasterId();
            RaftState state = initMsg.getState();
            if (initMsg.isConflict()) {
                LOGGER.warn("本地节点[{}]重新注册Master出现冲突", nodeId);
                return;
            }
            if (port < 0) { // 端口小于0代表本地节点已绑定端口, 走重连注册流程
                ClusterNode clusterNode = clusterCtx.getLocalNode();
                clusterNode.bindingPort(true);
                doRemoteCall(C2MRegisterNodeMsg.create(nodeId, true));
                LOGGER.info("节点重新初始化, 节点ID[{}], 节点类型[{}]",
                        clusterNode.getNodeId(), clusterNode.getNodeType());
                return;
            }
            clusterCtx.registerMasterActor(this, masterId, state);
            if (state == RaftState.LEADER) {
                InetSocketAddress address = new InetSocketAddress(port);
                ClusterNode clusterNode = ClusterNode.create(nodeId, nodeType, address)
                        .keepLocalIpList(getLocalAddresses()); // 持有本地节点的可连接IP
                clusterCtx.initLocalNode(clusterNode);
                boolean success = Clusters.startNode(center, clusterCtx, "");
                clusterNode.bindingPort(success);
                doRemoteCall(C2MRegisterNodeMsg.create(nodeId, success));
                LOGGER.info("节点初始化结果[{}], 分配节点ID[{}], 节点类型[{}]", success,
                        clusterNode.getNodeId(), clusterNode.getNodeType());
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void connectNodes(M2CNodeListMsg nodeListMsg) {
        try {
            LinkedList<NodeData> nodeList = nodeListMsg.getDataList();
            ctxAccess.contrastSurvivingNodesFromMaster(clusterCtx, nodeList); // 检测节点列表是否同步
            if (!nodeListMsg.getIpGroups().isEmpty()) {
                clusterCtx.resolveNodeIpGroups(nodeListMsg.getIpGroups());
            }
            for (NodeData data : nodeList) {
                connectNodeCheck(data, false);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void connectNode(M2CNodeMsg nodeMsg) {
        try {
            if (nodeMsg.isNodeJoins()) {
                connectNodeCheck(nodeMsg.getNodeData(), true);
                if (!nodeMsg.getIpGroups().isEmpty()) {
                    clusterCtx.resolveNodeIpGroups(nodeMsg.getIpGroups());
                }
            } else if (nodeMsg.isNodeExits()) {
                // 暂时什么都不干 TODO
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    // markPrepared 标记Actor准备状态
    private void connectNodeCheck(NodeData data, boolean markPrepared) {
        if (data.getNodeId() == clusterCtx.getLocalNode().getNodeId()) {
            if (markPrepared) {
                prepared = true;
                notifyRegisterAgain();
            }
            return;
        }
        if (clusterCtx.isConnectedNode(data.getNodeId())) {
            return;
        }
        int selfType = clusterCtx.getLocalNode().getNodeType();
        int targetType = data.getNodeType();
        InterconnectionStrategy targetStrategy = clusterCtx.getInterconnectionStrategy(targetType);
        InterconnectionStrategy selfStrategy = clusterCtx.getInterconnectionStrategy(selfType);

        if (NodeMainType.isInterConnect(selfStrategy, targetStrategy)) {
            Clusters.connectNode(data, clusterCtx);
        }
    }

    private void notifyRegisterAgain() {
        LOGGER.info("节点注册集群成功, 通知业务订阅者处理回调逻辑");
        Queue<MasterReconnectFuture> futures = callbackHolder.reconnectFutures;
        for ( ; futures.peek() != null; ) {
            MasterReconnectFuture future = futures.poll();
            future.succeedAndExecute(); // 重新注册Master成功, 通知关注的业务
        }
    }

    private void faultVote(M2CStartFaultVote message) {
        try {
            if (clusterCtx.isVoteTargetNodeFault(message)) {
                ClusterNode selfNode = clusterCtx.getLocalNode();
                int localNode = selfNode.getNodeId();
                int targetNode = message.getTargetNodeId();
                doRemoteCall(C2MNodeFaultVote.create(localNode, targetNode));

                int targetType = message.getTargetNodeType();
                LOGGER.info("收到投票通知, 本地节点({},{})检测到对端节点({},{})可能故障, 投票",
                        localNode, selfNode.getNodeType(), targetNode, targetType);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void judgeNodeFault(NodeFaultMsg message) {
        try {
            ctxAccess.nodeFailure(clusterCtx, message);
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void doSwitchoverMaster(M2CSwitchoverMaster message) {
        try {
            int selectedMasterId = message.getMasterId();
            if (message.isLeaderOrNot()) {
                LOGGER.info("收到通知主Master[{}]可用, 请等待重连操作", selectedMasterId);
                standbyMasterId = 0;
            } else {
                callbackHolder.defaultSuccessCallback(C2MSwitchoverMaster.class);
                if (standbyMasterId > 0 && standbyMasterId == selectedMasterId) {
                    return;
                }
                standbyMasterId = selectedMasterId;
                LOGGER.info("切换到可用从Master[{}], 准备节点初始化", standbyMasterId);

                // 逻辑到这里关键消息列表不可能为空, init node
                C2MInitNodeMsg initNodeMsg = (C2MInitNodeMsg) keyMessages.peek();
                ClusterNode localNode = clusterCtx.getLocalNode();
                if (localNode == null) {
                    doRemoteCall(initNodeMsg);
                } else {
                    int port = localNode.getAddress().getPort();
                    doRemoteCall(initNodeMsg.existingNode(localNode.getNodeId(), port));
                }
            }
            disconnectedCount = 0;
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void heartbeat(HeartbeartMsg message) {
        if (message.getIdInt() <= 0) {
            heartbeatTimeouts = 0;
            LOGGER.trace("收到Master服务心跳返回");
        } else {
            callbackHolder.defaultSuccessCallback(message.getIdInt());
        }
    }

    private void doFaultDetection(FaultTicker ticker) {
        try {
            if (client.isConnect()) {
                if (heartbeatTimeouts >= HEART_BEAT_TIMEOUTS) {
                    client.disconnect();
                    prepared = false;
                    LOGGER.info("Master服务心跳超时次数超过限制, 尝试断开重连");
                } else {
                    client.writeAndFlush(HeartbeartMsg.create());
                    heartbeatTimeouts ++; // 假定心跳超时
                    LOGGER.trace("向Master服务发送心跳消息");
                }
                return;
            }

            prepared = prepared & (standbyMasterId > 0);
            if (client.reconnect()) {
                if (keyMessages.peek() == null) {
                    return;
                }
                standbyMasterId = 0;
                C2MInitNodeMsg initNodeMsg = (C2MInitNodeMsg) keyMessages.peek(); // 必定是初始化消息
                ClusterNode localNode = clusterCtx.getLocalNode();
                if (localNode == null) {
                    doRemoteCall(initNodeMsg);
                } else {
                    int port = localNode.getAddress().getPort();
                    doRemoteCall(initNodeMsg.existingNode(localNode.getNodeId(), port));
                }
                heartbeatTimeouts = 0; // 清空心跳超时次数
            } else {
                if (followerClients.isEmpty()) {
                    return;
                }
                MasterClient standyClient = followerClients.get(standbyMasterId);
                if (standyClient != null) {
                    int id = callbackHolder.associateIdAndCallback(GENERIC_TIMEOUT,
                            ((ICallback) data -> {}).exceptionally(timeout -> {
                                disconnectedCount ++;
                                prepared = false;
                                LOGGER.debug("Master[{}]心跳超时", standbyMasterId);
                            }));
                    HeartbeartMsg heartbeat = HeartbeartMsg.create().injectId(id);
                    standyClient.writeAndFlush(heartbeat);
                } else {
                    disconnectedCount ++;
                }
                if (disconnectedCount < NUMBER_OF_FAILURES) {
                    return;
                }
                Iterator<MasterClient> it = followerClients.values().iterator();
                trySwitchoverMaster(it);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void trySwitchoverMaster(Iterator<MasterClient> sortIterator) {
        if (!sortIterator.hasNext()) {
            LOGGER.info("等待主Master重连, 尝试切换从Master服务器中...");
            return;
        }
        MasterClient followerClient = sortIterator.next();
        if (!followerClient.isConnect()) {
            followerClient.reconnect();
        }
        C2MSwitchoverMaster switchoverMsg = C2MSwitchoverMaster.create();
        ClusterNode localNode = clusterCtx.getLocalNode();
        if (localNode == null) {
            ChannelId channelId = followerClient.getClient().getChannel().id();
            String idShortText = channelId.asShortText().toUpperCase();
            int channelIdInt = (int) Long.parseLong(idShortText, 16) & Integer.MAX_VALUE;
            switchoverMsg.newNode(channelIdInt);
        } else {
            int nodeId = localNode.getNodeId();
            int nodeType = localNode.getNodeType();
            switchoverMsg.initializedInfo(nodeId, nodeType);
        }
        int timeoutMillis = 35000; // 超时毫秒
        callbackHolder.associateMsgAndCallback(switchoverMsg, timeoutMillis,
                ((ICallback) data -> {}).exceptionally(timeout -> trySwitchoverMaster(sortIterator) ));
        followerClient.writeAndFlush(switchoverMsg);
    }

    private void doOtherOperation(Object message) {
        if (message instanceof CallbackMessage) {
            CallbackMessage callbackMsg = (CallbackMessage) message;
            callbackHolder.successCallback(
                    callbackMsg.callbackId(), callbackMsg.returnData());
        } else if (message instanceof MasterReconnectFuture) { // 传进来这个future就代表关注Master服务的状态
            callbackHolder.addReconnectFuture((MasterReconnectFuture) message);
        } else if (message instanceof TimeoutTicker) {
            callbackHolder.timeoutCallback();
        } else {
            LOGGER.warn("收到不合法的消息:{}", message);
        }
    }

    @Override
    public void preStart() throws Exception {
        LOGGER.info("Master Client Actor初始化启动-> {}", getSelf());
        this.callbackHolder = new ServCallbackHolder(getTimers());
        getTimers().startPeriodicTimer(FaultTicker.class, FAULT_TICKER, Duration.ofMillis(10000L));
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        LOGGER.error("Master Client Actor故障重启-> {}, Reason:{}", getSelf(), reason);
    }

    @Override
    public void postStop() throws Exception {
        LOGGER.info("Master Client Actor停止运行-> {}", getSelf());
    }


    private static class ServCallbackHolder {

        // actor定时器
        TimerScheduler actorTimer;
        // 唯一Id生产者
        AtomicInteger uniqueIds = new AtomicInteger();
        // 消息类独占式回调
        HashMap<Object, Pair<MasterResult, ICallback>> msgCallbacks = new HashMap<>();
        // 重连回调
        LinkedList<MasterReconnectFuture> reconnectFutures = new LinkedList<>();

        ServCallbackHolder(TimerScheduler timer) {
            this.actorTimer = timer;
        }

        void associateMsgAndCallback(MasterMessage message, int timeout, ICallback callback) {
            MasterResult result = MasterResult.create(timeout);
            Pair<MasterResult, ICallback> callbackResult = Pair.of(result, callback);
            associateCallbackPair(message.getClass(), callbackResult);
        }

        int associateIdAndCallback(int timeout, ICallback callback) {
            int id = uniqueIds.incrementAndGet() & Integer.MAX_VALUE;
            if (id == 0)
                id = uniqueIds.incrementAndGet() & Integer.MAX_VALUE;
            MasterResult result = MasterResult.create(timeout);
            Pair<MasterResult, ICallback> callbackResult = Pair.of(result, callback);
            associateCallbackPair(id, callbackResult);
            return id;
        }

        void associateCallbackPair(Object key, Pair<MasterResult, ICallback> callbackResult) {
            int previousSize = msgCallbacks.size();
            msgCallbacks.putIfAbsent(key, callbackResult);
            if (previousSize == 0 && msgCallbacks.size() == 1) {
                actorTimer.startPeriodicTimer("timeoutTicker", new TimeoutTicker(), Duration.ofMillis(5000L));
            }
        }

        void addReconnectFuture(MasterReconnectFuture future) {
            reconnectFutures.offer(future);
        }

        void successCallback(Object invokerKey, Object data) {
            try {
                Pair<MasterResult, ICallback> callbackResult = msgCallbacks.remove(invokerKey);
                if (callbackResult != null) {
                    MasterResult result = callbackResult.getKey();
                    result.set(data);
                    callbackResult.getValue().onCall(result);
                }
                checkIfCancelTimerOrNot();
            } catch (Exception e) {
                LOGGER.error("Master逻辑回调异常", e);
            }
        }

        void defaultSuccessCallback(Object invokerKey) {
            successCallback(invokerKey, "success");
        }

        void timeoutCallback() {
            try {
                Iterator<Pair<MasterResult, ICallback>> it = msgCallbacks.values().iterator();
                long now = System.currentTimeMillis();
                for ( ; it.hasNext(); ) {
                    Pair<MasterResult, ICallback> callbackResult = it.next();
                    MasterResult result = callbackResult.getKey();
                    if (result.getTimestamp() + result.getTimeout() > now) {
                        continue;
                    }
                    it.remove();
                    NodeException timeoutEx = new NodeException("Master message callback timeout");
                    ICallback callback = callbackResult.getValue();
                    callback.onCall(timeoutEx);
                }
                checkIfCancelTimerOrNot();
            } catch (Exception e) {
                LOGGER.error("Master超时回调异常", e);
            }
        }

        void checkIfCancelTimerOrNot() {
            if (msgCallbacks.isEmpty()) {
                actorTimer.cancel("timeoutTicker");
            }
        }

    }

    // 故障定时检测
    private static class FaultTicker {
    }

    // 超时定时器
    private static class TimeoutTicker {
    }
}
