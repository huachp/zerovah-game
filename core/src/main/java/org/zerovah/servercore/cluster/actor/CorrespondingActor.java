package org.zerovah.servercore.cluster.actor;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;
import org.zerovah.servercore.cluster.*;
import org.zerovah.servercore.cluster.base.NodeConnectedFuture;
import org.zerovah.servercore.cluster.master.MasterActorsPool;
import org.zerovah.servercore.cluster.message.*;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 集群客户端通信Actor
 *
 * @author huachp
 */
public class CorrespondingActor extends AbstractActorWithTimers implements Correspondence {

    public static final AtomicInteger ACTOR_COUNT_RECORD = new AtomicInteger();

    private static final int MAX_CALL_BACK_ENTRIES = 5000;

    private static final Logger LOGGER = LogManager.getLogger(CorrespondingActor.class);

    private static final FaultTicker FAULT_TICKER = new FaultTicker();

    static final ContextAccessRights ctxAccess = ContextAccessRights.access();

    private ClusterNodeClient client;

    private ClusterContext clusterCtx;

    private FaultDetectionRecord detectionRecord; // 故障检测记录(父节点)

    private boolean forceRecycle; // 强制回收(父节点状态)

    private List<ActorRef> childrenActorPool = new ArrayList<>(); // 父节点Actor所持有的子节点Actor引用

    // Actor子节点故障回调
    private LinkedHashMap<Long, ICallback> faultCallbacks = new LinkedHashMap<>(128, 0.75f, true);
    // Actor子节点专用线程安全队列
    private ConcurrentLinkedQueue<Object> delayedMessages = new ConcurrentLinkedQueue<>();

    private final Receive receive = ReceiveBuilder.create()
            .match(RemoteMessage.class, this::doRemoteCall)
            .match(CallbackMessage.class, this::doRemoteCall)
            .match(RegisterActorMsg.class, this::doRegister)
            .match(NodeFaultMsg.class, this::doNodeFault)
            .match(FaultTicker.class, this::doFaultDetection)
            .match(HeartbeartMsg.class, this::doHeartbeatResponse)
            .matchAny(this::doOtherOperation)
            .build();

    public CorrespondingActor(ClusterNodeClient client, ClusterContext ctx) {
        this.client = client;
        this.clusterCtx = ctx;
    }

    public static Props props(ClusterNodeClient client, ClusterContext ctx) {
        return Props.create(CorrespondingActor.class, client, ctx);
    }

    // ------------------------------- 开放接口 -----------------------------------

    @Override
    public boolean isFaultyNode() {
        return detectionRecord.isAssumedFailure();
    }

    @Override
    public boolean actuallyFaultyNode() {
        return detectionRecord.isJudgedFailure();
    }

    @Override
    public boolean isVoteTargetNodeFault() {
        return !client.isConnect() && detectionRecord.getConnectionFailures() > 0;
    }

    @Override
    public int connectedNodeType() {
        return client.getNodeType();
    }

    @Override
    public int connectedNodeId() {
        return client.getNodeId();
    }

    @Override
    public String connectedIp() {
        return client.getConnectedIp();
    }

    @Override
    public boolean isNodeUsable() {
        return detectionRecord.isUsable();
    }

    @Override
    public void markNodeUsable(boolean usable) {
        detectionRecord.markUsable(usable);
    }

    @Override
    public ActorRef newChildActorRef() {
        if (context() != null) {
            return getContext().actorOf(props(client, clusterCtx));
        }
        return null; // actor被销毁了
    }

    @Override
    public ActorRef childActor() {
        return getChildActorFromPool();
    }

    @Override
    public Actor transform() {
        return this;
    }

    // ------------------------------- 开放接口 -----------------------------------

    @Override
    public Receive createReceive() {
        return receive;
    }

    private void doRemoteCall(Object message) {
        rewriteDelayMessages();
        writeOneMessage(message);
        messageActive(message);
    }

    private void writeOneMessage(Object message) {
        client.writeAndFlush(message).addListener(f -> {
            if (!f.isSuccess()) {
                int nodeType = client.getNodeType();
                int nodeId = client.getNodeId();
                String outputLog = "数据发送节点({},{})失败, 消息:{}";
                if (f.cause() != null) {
                    LOGGER.error(outputLog, nodeType, nodeId, message, f.cause());
                } else {
                    LOGGER.warn(outputLog, nodeType, nodeId, message);
                }
                messageDelayed(message);
            }
        });
    }

    private void messageDelayed(Object message) {
        delayedMessages.offer(message);
    }

    private void messageActive(Object message) {
        if (message instanceof RemoteMessage) {
            long actorId = ((RemoteMessage) message).getActorId();
            faultCallbacks.get(actorId);
        }
    }

    private void doRegister(RegisterActorMsg registerMsg) {
        try {
            Integer nodeId = registerMsg.getNodeId();
            NodeConnectedFuture nodeFuture = clusterCtx.getNodeConnectedFuture();
            if (!registerMsg.isConnectedNode()) {
                client.connect(); // 正式开始连接
                clusterCtx.registerParentActor(client.getNodeType(), nodeId, this); // 连接成功后注册父节点Actor
                if (client.isConnect()) {
                    client.writeAndFlush(clusterCtx.getLocalNode()); // 告诉对端本地节点的信息
                }
                registerSelfActor(nodeId, nodeFuture);
            } else {
                registerSelfActor(nodeId, nodeFuture);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void registerSelfActor(Integer nodeId, NodeConnectedFuture nodeFuture) {
        if (nodeFuture.isServerConnected(client.getNodeType(), nodeId)) { // 先判定对端服务是否已连接成功
            clusterCtx.addParentActorToNodePool(client.getNodeType(), this);
            clusterCtx.nodeConnectedSuccess(client.getNodeType(), nodeId, 0); // 本地端连接成功并回调
        } else {
            RegisterActorMsg newRegisterMsg = RegisterActorMsg.create(nodeId, true);
            Duration duration = Duration.ofMillis(3000L);
            getTimers().startSingleTimer("ACTOR_REGISTER_TIMER", newRegisterMsg, duration);
        }
    }

    private void doNodeFault(NodeFaultMsg message) {
        try {
            if (message.isHandledByParentActor()) {
                if (detectionRecord.isHandlingFault()) {
                    LOGGER.info("节点({},{})故障正在处理中", client.getNodeId(), client.getNodeType());
                    return;
                }
                detectionRecord.judgeNodeFailure(); // 判定此节点故障
                message.thenHandledByChildActor();
                int count = getChildrenCount();
                detectionRecord.readyPendingHandledCallbacks(count);
                message.detectionRecord(detectionRecord); // 注入检测记录

                Iterable<ActorRef> children = getContext().getChildren();
                for (ActorRef child : children) {
                    child.tell(message, self()); // 把父节点Actor的引用传入
                }
                forceRecycle = message.isForceRecycle(); // 用于判定是否强制回收Actor
            }
            if (message.isHandledByChildActor()) {
                callbackDelayMessages();
                for (ICallback faultCallback : faultCallbacks.values()) {
                    faultCallback.onCall(clusterCtx);
                }
                message.getRecord().handledCallbacksCountDown(); // 记录回调已处理
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private int getChildrenCount() {
        int count = 0;
        Iterable<ActorRef> children = getContext().getChildren();
        for (Iterator it = children.iterator(); it.hasNext();) {
            it.next(); count ++;
        }
        return count;
    }

    private void rewriteDelayMessages() {
        int count = delayedMessages.size();
        for (int i = 0; i < count; i++) {
            Object message = delayedMessages.poll();
            writeOneMessage(message);
        }
    }

    private void callbackDelayMessages() {
        for (Iterator<Object> it = delayedMessages.iterator(); it.hasNext(); ) {
            Object delayedMsg = it.next();
            if (delayedMsg instanceof RemoteMessage) {
                it.remove();
                long actorId = ((RemoteMessage) delayedMsg).getActorId();
                ICallback callback = faultCallbacks.get(actorId);
                if (callback instanceof ClusterActor.FindNodeCallback) {
                    ((ClusterActor.FindNodeCallback) callback).getOrInitResendQueue().offer(delayedMsg);
                }
            }
        }
    }

    private void doFaultDetection(FaultTicker ticker) {
        try {
            if (forceRecycle) {
                if (detectionRecord.isHandledAllFaultCallbacks()) {
                    LOGGER.info("强制回收本地异常父节点:{}", getSelf());
                    exitsClusterAndRecycle();
                    return;
                }
            }
            if (client.isConnect()) {
                if (detectionRecord.isHeartbeatTimeoutsExceededLimit()) {
                    client.disconnect();
                    LOGGER.info("节点({},{})心跳超时次数超过限制, 尝试断开重连",
                            client.getNodeId(), client.getNodeType());
                } else if (detectionRecord.isAppearedProblemWithHeartbeat()) {
                    client.disconnect();
                    doNodeFault(NodeFaultMsg.create(client.getNodeId(), true));
                    LOGGER.info("节点({},{})心跳出现问题, 估计本地节点状态出现异常",
                            client.getNodeId(), client.getNodeType());
                } else {
                    sendHeartbeat();
                }
                return;
            }
            if (detectionRecord.isJudgedFailure()
                    && detectionRecord.isHandledAllFaultCallbacks()) {
                exitsClusterAndRecycle();
                return;
            }
            if (client.reconnect()) {
                client.writeAndFlush(clusterCtx.getLocalNode()); // 告诉对端本地节点的信息
                detectionRecord.resetConnectionFailures();
                sendHeartbeat(); // 重连成功再发多一次心跳
            } else {
                detectionRecord.addConnectionFailures();
                if (detectionRecord.isInitiateFaultElection()) {
                    LOGGER.info("节点({},{})可能已经故障, 向Master服务发起故障选举",
                            client.getNodeId(), client.getNodeType());
                    ClusterNode localNode = clusterCtx.getLocalNode();
                    int faultNodeId = client.getNodeId();

                    MasterActorsPool masterActors = ctxAccess.getMasteActorPool(clusterCtx);
                    masterActors.tell(C2MInitiateFaultElection.create(localNode, faultNodeId));
                }
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void sendHeartbeat() {
        NodeConnectedFuture nodeFuture = clusterCtx.getNodeConnectedFuture();
        if (nodeFuture.isNodeInterconnected(client.getNodeType(), client.getNodeId())) {
            HeartbeartMsg heartbeat = HeartbeartMsg.create();
            FaultDetectionRecord.prepareHeartbeat(self(), heartbeat);
            client.writeAndFlush(heartbeat);
            LOGGER.trace("向节点[{}]发送心跳消息", client.getNodeId());
        }
    }

    private void exitsClusterAndRecycle() {
        int nodeId = client.getNodeId();
        int nodeType = client.getNodeType();
        LOGGER.info("节点({},{})故障, 已通知所有业务层回调处理, 开始移除节点", nodeId, nodeType);
        clusterCtx.nodeExitsCluster(nodeType, this);
        self().tell(PoisonPill.getInstance(), self()); // 自杀
    }

    private void doHeartbeatResponse(HeartbeartMsg message) {
        try {
            if (message.isRequest()) {
                client.writeAndFlush(message.response());
            } else if (message.isResponse()) {
                detectionRecord.resetHeartbeatTimeouts();
                detectionRecord.resetFailure(); // 心跳正常, 故障状态重置
                LOGGER.trace("收到节点[{}]心跳消息返回", client.getNodeId());
            } else if (message.isTimeout()) {
                detectionRecord.addHeartbeatTimeouts();
                LOGGER.info("节点[{}]心跳消息超时, 记录", client.getNodeId());
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void doOtherOperation(Object message) {
        try {
            if (message instanceof ClusterNode) { // 从Actor Server端传过来的
                ClusterNode node = (ClusterNode) message;
                clusterCtx.nodeConnectedSuccess(node.getNodeType(), node.getNodeId(), 1);
                // 注意, 这里的client对象一定是null
            } else if (message instanceof ClusterActor.ActorIdStateMsg) {
                ClusterActor.ActorIdStateMsg stateMsg = (ClusterActor.ActorIdStateMsg) message;
                if (stateMsg.isDestroy()) {
                    faultCallbacks.remove(stateMsg.getActorId());
                } else {
                    faultCallbacks.put(stateMsg.getActorId(), stateMsg.getFailedCallback());
                    if (faultCallbacks.size() > MAX_CALL_BACK_ENTRIES) {
                        Iterator<Long> it = faultCallbacks.keySet().iterator();
                        faultCallbacks.remove(it.next()); // remove first
                    }
                }
            } else {
                LOGGER.warn("收到不合法的消息:{}", message);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private ActorRef getChildActorFromPool() {
        int capacity = NettyRuntime.availableProcessors() * 2;
        if (childrenActorPool.size() < capacity) {
            synchronized (this) {
                if (childrenActorPool.size() < capacity) {
                    ActorRef childActor = newChildActorRef();
                    childrenActorPool.add(childActor);
                    return childActor;
                }
            }
        }
        int randomNum = ThreadLocalRandom.current().nextInt(0, capacity);
        return childrenActorPool.get(randomNum);
    }

    @Override
    public void preStart() throws Exception {
        ACTOR_COUNT_RECORD.incrementAndGet(); // 通信Actor对象计数
        String actorName = getSelf().path().name();
        String parentName = ctxAccess.clientActorParentName(clusterCtx);
        if (actorName.contains(parentName)) {
            detectionRecord = new FaultDetectionRecord();
            getTimers().startPeriodicTimer(FaultTicker.class, FAULT_TICKER, Duration.ofMillis(5000L));
        }
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        LOGGER.error("通信Actor故障重启-> {}, Reason:{}", getSelf(), reason);
    }

    @Override
    public void postStop() throws Exception {
        ACTOR_COUNT_RECORD.decrementAndGet(); // 通信Actor对象计数

        String actorName = getSelf().path().name();
        String parentName = ctxAccess.clientActorParentName(clusterCtx);
        if (actorName.contains(parentName)) {
            client.disconnect(); // 父Actor销毁时, 确保连接断开
            clusterCtx.removeParentActor(client.getNodeId(), this);
        }
    }


    // 故障定时检测
    private static class FaultTicker {

    }

}

