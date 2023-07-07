package org.zerovah.servercore.cluster.actor;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ScalaActorRef;
import org.zerovah.servercore.cluster.ClusterContext;
import org.zerovah.servercore.cluster.ContextAccessRights;
import org.zerovah.servercore.cluster.ICallback;
import org.zerovah.servercore.cluster.base.ActorIdGenerator;
import org.zerovah.servercore.cluster.cause.NodeException;
import org.zerovah.servercore.cluster.message.AbstractRemoteMsg;
import org.zerovah.servercore.cluster.message.CallbackTimeoutCDMsg;
import org.zerovah.servercore.cluster.message.RemoteMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 集群Actor
 *
 * @author huachp
 */
public class ClusterActor {

    private static final Logger LOGGER = LogManager.getLogger(ClusterActor.class);

    private static final ActorIdGenerator GENERATOR = new ActorIdGenerator();

    static final ContextAccessRights ctxAccess = ContextAccessRights.access();

    static class FindNodeCallback implements ICallback {

        private static final ActorRef EMPTY_ACTORREF = new EmptyActorRef();

        /** 失败处理中 */
        private final AtomicBoolean failureHanding = new AtomicBoolean(false);
        /** 失败处理后需要重发的消息 */
        private ConcurrentLinkedQueue<Object> resendMessages;

        private long actorId;
        private int nodeType;
        private ActorRef newRemoteActor = EMPTY_ACTORREF;

        FindNodeCallback(long actorId, int nodeType) {
            this.actorId = actorId;
            this.nodeType = nodeType;
        }

        @Override
        public void onCall(Object data) {
            if (!failureHanding.getAndSet(true)) {
                synchronized (this) {
                    newRemoteActor = null; // 先置空
                    ClusterContext clusterCtx = (ClusterContext) data;
                    ActorRef newActor = reacquireNewActor(clusterCtx);
                    LOGGER.info("节点[{}]故障, 重新连接可用节点:{}",
                            nodeType, newActor);
                    if (newActor == null) {
                        LOGGER.warn("警告: 暂无可用[{}]类型节点", nodeType);
                        resendMessages = null; // 因为找不到有可用的节点了, 跳过消息重发
                        return;
                    }
                    newRemoteActor = newActor;
                    newRemoteActor.tell(ActorIdStateMsg.stateNew(actorId, this), ActorRef.noSender());
                    if (resendMessages != null) {
                        for (Object message : resendMessages) {
                            newRemoteActor.tell(message, ActorRef.noSender());
                        }
                    }
                }
            }
        }

        ConcurrentLinkedQueue<Object> getOrInitResendQueue() {
            if (resendMessages == null) {
                resendMessages = new ConcurrentLinkedQueue<>();
            }
            return resendMessages;
        }

        ActorRef reacquireNewActor(ClusterContext clusterCtx) {
            return ctxAccess.getNewChildActor(clusterCtx, nodeType);
        }

        ActorRef getNewActor(ClusterContext clusterCtx) {
            if (newRemoteActor == null) {
                ActorRef newActor = reacquireNewActor(clusterCtx);
                if (newActor == null) {
                    return null;
                }
                newRemoteActor = newActor;
                newRemoteActor.tell(ActorIdStateMsg.stateNew(actorId, this), ActorRef.noSender());
            }
            failureHanding.set(false);
            return newRemoteActor;
        }

        boolean isNodeFailure() {
            return failureHanding.get();
        }

    }

    static class FastFailCallback implements ICallback {

        /** 故障业务层处理 */
        private final Queue<ICallback> failureCallbacks;

        FastFailCallback(ICallback actualCallback) {
            this.failureCallbacks = new ConcurrentLinkedQueue<>();
            addCallback(actualCallback);
        }

        void addCallback(ICallback actualCallback) {
            this.failureCallbacks.offer(actualCallback);
        }

        @Override
        public void onCall(Object data) {
            Iterator<ICallback> it = failureCallbacks.iterator();
            for ( ; it.hasNext(); ) {
                it.next().onCall(data);
            }
        }
    }

    static class EmptyActorRef extends ActorRef implements ScalaActorRef {

        @Override
        public ActorPath path() {
            return null;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public void $bang(Object message, ActorRef sender) {
            LOGGER.info("EmptyActorRef不能发送消息:{}", message);
        }
    }


    /** 集群上下文 */
    protected ClusterContext clusterContext;
    /** 远程节点Actor引用 */
    protected volatile ActorRef remoteActorRef;
    /** 目标节点类型 */
    protected int nodeType = -1;
    /** 目标节点Id */
    protected int nodeId = -1;
    /** 是否自动寻找下一个节点 */
    private boolean autoFindNextNode;
    /** 主动销毁自身没有效果 */
    private final boolean noEffectByDestroy;

    private ICallback failedCallback;

    private long actorId;

    public ClusterActor() {
        this.noEffectByDestroy = false;
    }

    public ClusterActor(boolean noEffectByDestroy) {
        this.noEffectByDestroy = noEffectByDestroy;
    }

    public ClusterContext getClusterContext() {
        return clusterContext;
    }

    public int parseNodeId() {
        ActorPath actorPath = remoteActorRef.path().parent(); // 父节点路径
        String[] elements = actorPath.name().split("-");
        String nodeIdStr = elements[elements.length - 1];
        String[] nodeIdArray = nodeIdStr.split("\\.");
        return Integer.parseInt(nodeIdArray[0]);
    }

    public void call(RemoteMessage remoteMsg, ICallback callback) {
        doWhenRemoteNodeFault();
        if (remoteActorRef == null) {
            return;
        }
        AbstractRemoteMsg subMessage = (AbstractRemoteMsg) remoteMsg;
        ActorRef workingActor = subMessage.workingActor();
        int callbackId = CallbackCenter.get().futureCallback(workingActor, callback, remoteMsg.getClass());
        int timeoutSenconds = subMessage.getTimeoutTime();
        if (timeoutSenconds > 0) {
            CallbackTimeoutCDMsg timeoutMsg = CallbackTimeoutCDMsg.create(callbackId, subMessage);
            if (workingActor != null) {
                workingActor.tell(timeoutMsg, ActorRef.noSender());
            } else {
                BaseMessageActor.timeoutMonitoring(timeoutMsg);
            }
        }
        int crossNodeId = nodeId >> ActorIdGenerator.NODEID_BITS;
        subMessage.sourceNodeId(clusterContext.getLocalNode()); // 源节点ID
        subMessage.clusterActorId(actorId); // 带上actorId到逻辑服务端
        subMessage.determinedNodeId(crossNodeId);
        subMessage.associateId(callbackId);
        remoteActorRef.tell(subMessage, ActorRef.noSender());
    }

    private void doWhenRemoteNodeFault() {
        if (autoFindNextNode && failedCallback != null) {
            FindNodeCallback findNodeCallback = (FindNodeCallback) failedCallback;
            if (!findNodeCallback.isNodeFailure()) {
                return;
            }
            synchronized (findNodeCallback) {
                if (findNodeCallback.isNodeFailure()) {
                    LOGGER.info("远程节点Actor({})因故障失效", remoteActorRef);
                    this.remoteActorRef = null;
                }
                ActorRef newActorRef = findNodeCallback.getNewActor(clusterContext);
                if (newActorRef != null) {
                    this.remoteActorRef = newActorRef;
                }
            }
        }
    }

    public void tell(RemoteMessage remoteMsg) {
        doWhenRemoteNodeFault();
        if (remoteActorRef == null) {
            return;
        }
        AbstractRemoteMsg subMessage = (AbstractRemoteMsg) remoteMsg;
        int crossNodeId = nodeId >> ActorIdGenerator.NODEID_BITS;
        subMessage.sourceNodeId(clusterContext.getLocalNode()); // 源节点ID
        subMessage.clusterActorId(actorId); // 带上actorId到逻辑服务端
        subMessage.determinedNodeId(crossNodeId);
        remoteActorRef.tell(subMessage, ActorRef.noSender());
    }

    public void destroy() {
        if (noEffectByDestroy) { // 销毁无效
            remoteActorRef = null;
            clusterContext = null;
            failedCallback = null;
            return;
        }
        if (remoteActorRef == null) {
            return;
        }
        remoteActorRef.tell(ActorIdStateMsg.stateDestroy(actorId), ActorRef.noSender());
        remoteActorRef = null;
        clusterContext = null;
        failedCallback = null;
    }

    /**
     * 节点故障处理策略, 故障自动查找可用的同类型节点, 延迟消息做切线处理(适合无状态服务节点, 如果节点服务是有状态的, 会丢失所有状态)
     *
     * @return ClusterActor
     */
    public ClusterActor failureAutoFindNextNode() {
        if (noEffectByDestroy) {
            return this;
        }
        if (failedCallback != null) {
            return this;
        }
        FindNodeCallback findNodeCallback = new FindNodeCallback(actorId, nodeType);
        remoteActorRef.tell(ActorIdStateMsg.stateNew(actorId, findNodeCallback), ActorRef.noSender());
        autoFindNextNode = true;
        failedCallback = findNodeCallback;
        return this;
    }

    /**
     * 节点故障处理策略, 故障后快速通知业务层回调处理(适合有状态服务节点)
     *
     * @param callback 当节点判定故障, 通知业务层回调
     * @return ClusterActor
     */
    public ClusterActor failureFastNotify(ICallback callback) {
        if (noEffectByDestroy) {
            return this;
        }
        if (autoFindNextNode) {
            LOGGER.warn("{}逻辑上已调用failureAutoFindNextNode, 不允许调用failureFastNotify", this);
            return this;
        }
        if (failedCallback != null) {
            ((FastFailCallback) failedCallback).addCallback(callback);
            return this;
        }
        FastFailCallback fastFailCallback = new FastFailCallback(callback);
        remoteActorRef.tell(ActorIdStateMsg.stateNew(actorId, fastFailCallback), ActorRef.noSender());
        failedCallback = fastFailCallback;
        return this;
    }


    public static ClusterActor createWithNodeType(ClusterContext ctx, ActorRef remoteActor, int nodeType) {
        ClusterActor actor = create(ctx, remoteActor);
        actor.nodeType = nodeType;
        return actor;
    }

    public static ClusterActor createWithNodeId(ClusterContext ctx, ActorRef remoteActor, int nodeId) {
        ClusterActor actor = create(ctx, remoteActor);
        actor.nodeId = nodeId;
        return actor;
    }

    static ClusterActor create(ClusterContext ctx, ActorRef remoteActor) {
        ClusterActor actor = new ClusterActor();
        actor.clusterContext = ctx;
        actor.remoteActorRef = remoteActor;
        actor.actorId = GENERATOR.nextId(ctx.getLocalNode().getNodeId());
        return actor;
    }

    public static ClusterActor createNotBeDestroy(ClusterContext ctx, ActorRef remoteActor, int nodeType) {
        ClusterActor actor = new ClusterActor(true);
        actor.clusterContext = ctx;
        actor.remoteActorRef = remoteActor;
        actor.nodeType = nodeType;
        actor.actorId = GENERATOR.nextId(ctx.getLocalNode().getNodeId());
        return actor;
    }

    static class ActorIdStateMsg {

        private long actorId;
        private ICallback failedCallback;
        private boolean destroy;

        long getActorId() {
            return actorId;
        }

        ICallback getFailedCallback() {
            return failedCallback;
        }

        boolean isDestroy() {
            return destroy;
        }

        static ActorIdStateMsg stateNew(long actorId, ICallback callback) {
            ActorIdStateMsg message = new ActorIdStateMsg();
            message.actorId = actorId;
            message.failedCallback = callback;
            return message;
        }

        static ActorIdStateMsg stateDestroy(long actorId) {
            ActorIdStateMsg message = new ActorIdStateMsg();
            message.actorId = actorId;
            message.destroy = true;
            return message;
        }

    }

    static class OneWayClusterActor extends ClusterActor {

        public OneWayClusterActor(ActorRef sender) {
            this.remoteActorRef = sender;
        }

        @Override
        public void call(RemoteMessage remoteMsg, ICallback callback) {
            throw new UnsupportedOperationException("OneWayClusterActor unsupport remoteCall");
        }

        @Override
        public void tell(RemoteMessage remoteMsg) {
            justTell(remoteMsg);
        }

        @Override
        public ClusterActor failureAutoFindNextNode() {
            return this;
        }

        @Override
        public ClusterActor failureFastNotify(ICallback callback) {
            return this;
        }

        @Override
        public void destroy() {
            throw new UnsupportedOperationException("OneWayClusterActor unsupport destroy");
        }

        public void justTell(RemoteMessage remoteMsg) {
            this.remoteActorRef.tell(remoteMsg, ActorRef.noSender());
        }

    }

    static class EmptyClusterActor extends ClusterActor {

        static final EmptyClusterActor DEFAULT = new EmptyClusterActor();

        public EmptyClusterActor() {
        }

        @Override
        public void call(RemoteMessage remoteMsg, ICallback callback) {
            NodeException exception = new NodeException("node failure");
            callback.onCall(exception);
        }

        @Override
        public void tell(RemoteMessage remoteMsg) {
            LOGGER.info("检测到对端节点已故障, 消息发送失败, {}", remoteMsg);
        }

        @Override
        public ClusterActor failureAutoFindNextNode() {
            return this;
        }

        @Override
        public ClusterActor failureFastNotify(ICallback callback) {
            return this;
        }
    }

    public static EmptyClusterActor createEmpty() {
        return EmptyClusterActor.DEFAULT;
    }

}
