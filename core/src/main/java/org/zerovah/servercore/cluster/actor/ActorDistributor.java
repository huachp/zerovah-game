package org.zerovah.servercore.cluster.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import org.zerovah.servercore.akka.AkkaPathContext;
import org.zerovah.servercore.cluster.*;
import org.zerovah.servercore.cluster.base.NodeMainType;
import org.zerovah.servercore.cluster.message.CallbackMessage;
import org.zerovah.servercore.cluster.message.HeartbeartMsg;
import org.zerovah.servercore.cluster.message.RemoteMessage;
import io.netty.util.NettyRuntime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 集群网络消息Actor分发器
 *
 * @author huachp
 */
public class ActorDistributor implements MessageDistributor {

    private static final Logger LOGGER = LogManager.getLogger(ActorDistributor.class);

    private static final String FIXED_ACTOR_NAME = "cluster-fixed-actor-";

    static final ContextAccessRights ctxAccess = ContextAccessRights.access();

    /** 固定集群消息处理Actors */
    private ActorRef[] processingActors;
    /** 业务Actor与集群消息绑定映射 */
    private ConcurrentMap<Long, ActorRef> bindingActors = new ConcurrentHashMap<>();
    /** 集群上下文 */
    private ClusterContext clusterCtx;
    /** 消息枢纽中心处理器 */
    private HubHandler hubHandler;

    public ActorDistributor(int actorCount) {
        int length = actorCount == 0 ? NettyRuntime.availableProcessors() : actorCount;
        this.processingActors = new ActorRef[length];
    }

    public void prepareFixedActors(AkkaPathContext akkaCtx) {
        ActorRef firstActor = processingActors[0];
        if (firstActor != null) {
            LOGGER.warn("节点消息Actor已准备完毕, 不需要创建新的节点Actor");
            return;
        }
        for (int i = 0; i < processingActors.length; i++) {
            String actorName = FIXED_ACTOR_NAME + i;
            ActorRef fixedActor = akkaCtx.clusterActor(actorName, BaseMessageActor.class);
            processingActors[i] = fixedActor;
        }
        LOGGER.info("创建基础节点消息Actor个数:{}, Actor名字标识:{}",
                processingActors.length, FIXED_ACTOR_NAME);
    }

    @Override
    public void distribute(int nodeId, Object message) {
        if (message instanceof RemoteMessage) {
            distributeRemoteMsg(nodeId, (RemoteMessage) message);
        } else if (message instanceof CallbackMessage) {
            distributeCallback((CallbackMessage) message);
        } else if (message instanceof HeartbeartMsg) {
            distributeHeartbeat(nodeId, (HeartbeartMsg) message);
        } else {
            LOGGER.error("Actor分发器收到不合法的消息[{}]", message);
        }
    }

    @Override
    public int checkNode(Object message) {
        if (message instanceof ClusterNode) {
            ClusterNode clusterNode = (ClusterNode) message;
            ActorSelection actorSelection = clusterCtx.seekFirstConnectedActor(false);
            actorSelection.tell(clusterNode, ActorRef.noSender());
            return clusterNode.getNodeId();
        }
        return -1;
    }

    private void distributeRemoteMsg(int nodeId, RemoteMessage message) {
        long actorId = message.getActorId();
        if (actorId < 0) {
            throw new IllegalArgumentException("不合法的消息actor id: " + actorId);
        }
        int actorHash = Long.hashCode(message.getActorId());
        ActorRef bussinessActor = bindingActors.get(actorId);
        if (bussinessActor != null) {
            ActorRef clientActor = ctxAccess.getClusterActorByNodeId(clusterCtx, nodeId, actorHash);
            bussinessActor.tell(message, clientActor);
            return;
        }
        ActorRef clientActor = ctxAccess.getClusterActorByNodeId(clusterCtx, nodeId, actorHash);
        ActorRef processingActor = chooseFixedActor(message);
        processingActor.tell(message, clientActor);
    }

    private ActorRef chooseFixedActor(RemoteMessage message) {
        long actorId = message.getActorId();
        int hash = Long.hashCode(actorId);
        hash = hash ^ (hash >>> 12);
        int refIndex = hash & (processingActors.length - 1); // 计算数组下标
        ActorRef actorRef = processingActors[refIndex];
        return actorRef;
    }

    private void distributeCallback(CallbackMessage message) {
        if (relayTransfer(message)) {
            return;
        }
        int callbackId = message.callbackId();
        int actorIdHash = Long.hashCode(message.senderActorId());
        int hash = actorIdHash <= 0 ? callbackId : actorIdHash ^ (actorIdHash >>> 12);
        int refIndex = hash & (processingActors.length - 1); // 计算数组下标
        ActorRef defaultActor = processingActors[refIndex];
        CallbackCenter.get().processCallback(message, defaultActor);
    }

    private boolean relayTransfer(CallbackMessage message) {
        if (hubHandler == null) {
            return false;
        }
        long actorId = message.senderActorId();
        long highBitMark = actorId >> 31; // 高位标识
        int nodeType = clusterCtx.getLocalNode().getNodeType();
        InterconnectionStrategy strategy = clusterCtx.getInterconnectionStrategy(nodeType);
        if (strategy.nodeType() == NodeMainType.RELAY && highBitMark > 0) {
            hubHandler.forward(message);
            return true;
        }
        return false;
    }

    private void distributeHeartbeat(int nodeId, HeartbeartMsg message) {
        int refIndex = message.hashCode() & (processingActors.length - 1); // 计算数组下标
        ActorRef actorRef = processingActors[refIndex];
        ActorRef clientActor = ctxAccess.getClusterActorByNodeId(clusterCtx, nodeId);
        actorRef.tell(message, clientActor);
    }

    public void bindingActor(Long actorId, ActorRef actorRef) {
        ActorRef oldRef = bindingActors.put(actorId, actorRef);
        if (oldRef != null) {
            LOGGER.warn("clientActorId={}之前有绑定oldActor[{}], 现已替换成newActor[{}], 请注意检查逻辑是否出错",
                    actorId, oldRef, actorRef);
        }
    }

    public void unbinding(Long actorId, ActorRef unbdingActor) {
        ActorRef nowBinding = bindingActors.get(actorId);
        if (nowBinding == unbdingActor) {
            bindingActors.remove(actorId);
        }
    }

    // 注意: 节点服务启动时调用
    public ActorDistributor cluster(ClusterContext ctx) {
        if (this.clusterCtx == null) {
            ctx.seekFirstConnectedActor(true);
            this.clusterCtx = ctx;
        }
        return this;
    }

    public ActorDistributor relayHub(HubHandler handler) {
        this.hubHandler = handler;
        return this;
    }


    public static ActorDistributor create(ActorSystem actorSys, int fixedCount) {
        BaseMessageActor.prepareMonitorActor(actorSys);
        if (!isPowerOfTwo(fixedCount)) {
            fixedCount = toPowerOfTwo(fixedCount); // 数组容量以2的N次方补全
        }
        ActorDistributor distributor = new ActorDistributor(fixedCount);
        return distributor;
    }

    private static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    private static int toPowerOfTwo(int val) {
        return Integer.highestOneBit(val) << 1;
    }

}
