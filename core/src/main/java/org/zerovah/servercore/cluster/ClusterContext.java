package org.zerovah.servercore.cluster;

import akka.actor.*;
import org.zerovah.servercore.cluster.MixedNodeActor.RemoteActorRef;
import org.zerovah.servercore.cluster.actor.*;
import org.zerovah.servercore.cluster.base.ActorIdGenerator;
import org.zerovah.servercore.cluster.base.NodeConnectedFuture;
import org.zerovah.servercore.cluster.base.NodeMainType;
import org.zerovah.servercore.cluster.master.MasterActorsPool;
import org.zerovah.servercore.cluster.master.MasterClient;
import org.zerovah.servercore.cluster.master.NodeData;
import org.zerovah.servercore.cluster.master.RaftState;
import org.zerovah.servercore.cluster.message.M2CStartFaultVote;
import org.zerovah.servercore.cluster.message.NodeFaultMsg;
import org.zerovah.servercore.cluster.message.RegisterActorMsg;
import io.netty.channel.EventLoopGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 集群上下文
 *
 * @author huachp
 */
public class ClusterContext {

    private static final Logger LOGGER = LogManager.getLogger(ClusterContext.class);

    private static final String CLIENT_AKKA_SYSTEM_NAME = "cluster-system";
    private static final String ACTOR_PARENT_NAME = "client-actor-parent-";
    private static final String MASTER_ACTOR_NAME = "master-client-actor-";

    /** ParentActorIns空对象占位符 */
    private static final ParentActorIns PLACEHOLDER = new ParentActorIns(null);


    /** 集群Actor消息客户端系统 */
    private ActorSystem clusterClientSys;
    /** 集群客户端IO线程组 */
    private EventLoopGroup clientThreadGroup;
    /** 本地节点 */
    private ClusterNode localNode;
    /** 所有关注的节点Actor, 不提供对外调用 */
    private ConcurrentMap<Integer, ParentActorIns> focusNodeActors = new ConcurrentHashMap<>();
    /** 集群节点信息({节点类型=节点客户端连接信息Actor}) */
    private ConcurrentMap<Integer, NodeActorsPool> nodes = new ConcurrentHashMap<>();
    /** Master通信Actor实例 */
    private MasterActorsPool masterActorPool = new MasterActorsPool();
    /** 节点互连策略{节点类型=互连策略} */
    private Map<Integer, InterconnectionStrategy> nodeInterconnStrategys;
    /** 节点第一次连接成功通知 */
    private NodeConnectedFuture nodeConnectedFuture;
    /** IP组配置数据 */
    private IpGroupData ipGroupData = new IpGroupData();

    /** 节点随机路由策略 */
    private RandomRouteStrategy randomStrategy = new RandomRouteStrategy();
    /** 节点一致性hash路由策略 */
    private ConsistentHashRouteStrategy consistentHashStrategy = new ConsistentHashRouteStrategy(this);

    public ClusterContext() {
    }

    private void initSystem() {
        clusterClientSys = ActorSystem.create(CLIENT_AKKA_SYSTEM_NAME);
    }

    public void initNodeStrategys(Map<Integer, InterconnectionStrategy> strategys) {
        if (this.nodeInterconnStrategys == null) {
            this.nodeInterconnStrategys = strategys;
        }
    }

    public void initLocalNode(ClusterNode localNode) {
        this.localNode = localNode;
    }

    public void initThreadGroup(EventLoopGroup eventLoopGroup) {
        if (clientThreadGroup == null) {
            clientThreadGroup = eventLoopGroup;
        } else {
            LOGGER.warn("重复初始化集群上下文客户端IO线程池:{}", eventLoopGroup);
        }
    }

    void registerParentNode(Integer nodeId, ClusterNodeClient client) {
        ParentActorIns exist = focusNodeActors.putIfAbsent(nodeId, PLACEHOLDER);
        if (exist == null || exist == PLACEHOLDER) {
            ActorRef parentActor = createParentNodeActor(client, 0);
            parentActor.tell(RegisterActorMsg.create(nodeId), ActorRef.noSender());
        } else {
            if (exist.isDestroyInProgress()) {
                int h = exist.hashCode();
                int suffix = (h ^ (h >>> 16)) & Short.MAX_VALUE;
                ActorRef parentActor = createParentNodeActor(client, suffix);
                parentActor.tell(RegisterActorMsg.create(nodeId), ActorRef.noSender());
                focusNodeActors.put(client.getNodeId(), PLACEHOLDER);
            }
        }

    }

    public void registerParentActor(Integer nodeType, Integer nodeId, Correspondence parentActor) {
        NodeActorsPool actorPool = getNodePool(nodeType);
        actorPool.toLock();
        try {
            ParentActorIns actorIns = focusNodeActors.get(nodeId);
            if (actorIns != null && actorIns != PLACEHOLDER) {
                Actor actualActor = parentActor.transform(); // 转换
                actualActor.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
                LOGGER.warn("已存在相同节点ID[{}]的父节点Actor, 原有:{}, 传入:{}",
                        nodeId, actorIns.getParentIns(), parentActor);
                return;
            }
            focusNodeActors.put(nodeId, createParentActorIns(parentActor));
        } finally {
            actorPool.unLock();
        }
    }

    private NodeActorsPool getNodePool(Integer nodeType) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            actorPool = new NodeActorsPool();
            NodeActorsPool old = nodes.putIfAbsent(nodeType, actorPool);
            if (old != null) {
                actorPool = old;
            }
        }
        return actorPool;
    }

    public void addParentActorToNodePool(Integer nodeType, Correspondence parentActor) {
        NodeActorsPool actorPool = getNodePool(nodeType);
        actorPool.toLock();
        try {
            actorPool.addActor(parentActor);
        } finally {
            actorPool.unLock();
        }
    }

    private ParentActorIns createParentActorIns(Correspondence parentActor) {
        ActorRef firstChild = parentActor.newChildActorRef();
        ActorRef secondChild = parentActor.newChildActorRef();
        ParentActorIns actorIns = ParentActorIns.create(parentActor);
        actorIns.initChildren(firstChild, secondChild);
        return actorIns;
    }

    private ActorRef createParentNodeActor(ClusterNodeClient client, int suffix) {
        if (suffix == 0) {
            String actorName = ACTOR_PARENT_NAME + client.getNodeId();
            return clusterClientSys.actorOf(CorrespondingActor.props(client, this), actorName);
        } else {
            String actorName = ACTOR_PARENT_NAME + client.getNodeId() + "." + suffix;
            return clusterClientSys.actorOf(CorrespondingActor.props(client, this), actorName);
        }
    }

    ParentActorIns getParentActor(Integer nodeId) {
        Objects.requireNonNull(nodeId);
        return focusNodeActors.get(nodeId);
    }

    /**
     * 通过节点类型获取远程通信的节点Actor, 路由策略{@link RandomRouteStrategy}, 随机选取节点, 每次调本方法获取的Actor, 可能路由到不同的节点
     *
     * @param nodeType 节点类型, 业务层自定义
     * @return {@link ClusterActor}
     */
    public ClusterActor getClusterActorByNodeType(Integer nodeType) {
        ActorRef childActor = getNewChildActor(nodeType);
        if (childActor == null) {
            throw new NullPointerException("无可用的" + nodeType + "类型节点");
        }
        return ClusterActor.createWithNodeType(this, childActor, nodeType);
    }

    /**
     * 通过节点类型和业务层唯一标识key获取远程通信的节点Actor, 路由策略{@link ConsistentHashRouteStrategy}<p>
     * 以key值hash后在hash环中选取最近的节点, 封装成节点Actor返回给业务层, 只要保证key不变, 调用本方法获取的Actor路由到的节点不变
     *
     * @param nodeType 节点类型, 业务层自定义
     * @param key 用于一致性hash的key值
     * @return {@link ClusterActor}
     */
    public ClusterActor getClusterActorByNodeHash(Integer nodeType, int key) {
        return getConsistentHashChildActor(nodeType, key);
    }

    /**
     * 通过{@link NodeMainType}获取随机节点的远程通信ClusterActor, 该方法不建议业务层调用, 框架内部使用方法
     *
     * @param mainType 节点大类型
     * @return {@link ClusterActor}
     */
    public ClusterActor getClusterActorByMainType(NodeMainType mainType) {
        if (mainType == NodeMainType.BUSINESS) {
            throw new UnsupportedOperationException("不支持传入参数" + mainType);
        }
        Integer actualNodeType = -1;
        for (Map.Entry<Integer, InterconnectionStrategy> entry : nodeInterconnStrategys.entrySet()) {
            InterconnectionStrategy strategy = entry.getValue();
            if (strategy.nodeType() == mainType) {
                actualNodeType = entry.getKey(); break;
            }
        }
        if (actualNodeType == -1) {
            throw new NullPointerException("找不到" + mainType + "对应的节点类型");
        }
        return getClusterActorByNodeType(actualNodeType);
    }

    public ClusterActor determineClusterNodeActor(Integer nodeId) {
        int actualNodeId = nodeId & ActorIdGenerator.MAX_NODEID;
        ParentActorIns parentActorIns = focusNodeActors.get(actualNodeId);
        if (parentActorIns == null) {
            return ClusterActor.createEmpty(); // 返回默认空实例
        }
        Correspondence parentActor = parentActorIns.getCorrespondence();
        ActorRef actorRef = parentActor.childActor();
        return ClusterActor.createWithNodeId(this, actorRef, nodeId); // 确定节点ID的ClusterActor不携带nodeType
    }

    public MixedNodeActor getTypeDefinedMixedNodeActor(Integer nodeType) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            return MixedNodeActor.DEFAULT;
        }
        return actorPool.getBroadcastActor().localNode(localNode.getNodeId());
    }

    @Deprecated
    public CompositeClusterActor getBroadcastActorByNodeType(Integer nodeType) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            return CompositeClusterActor.create(MixedNodeActor.DEFAULT);
        }
        MixedNodeActor mixedActor = actorPool.getBroadcastActor().localNode(localNode.getNodeId());
        return CompositeClusterActor.get(nodeType, mixedActor);
    }

    public int otherNormalNodesCount(Integer nodeType) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            return 0;
        }
        return actorPool.normalNodesCount();
    }

    /**
     * 获取其他进程节点类型的Leader节点Actor, 如果传入的nodeType是和自身进程的节点类型相同, 则不返回有效的ClusterActor对象
     *
     * @param nodeType 节点类型, 不包括自身进程节点类型的其他节点类型
     * @return {@link ClusterActor}
     */
    public ClusterActor getLeaderActorFromOtherNodeType(int nodeType) {
        if (localNode.getNodeType() == nodeType) {
            throw new IllegalArgumentException("传入节点类型[" + nodeType + "]和本地节点类型相同, 请查看本方法注释说明");
        }
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            throw new NullPointerException("无可用的" + nodeType + "类型节点");
        }
        int leaderNodeId = actorPool.getMinimumNodeId();
        return determineClusterNodeActor(leaderNodeId);
    }

    /**
     * 通过节点类型获取所有该类型的远程通信的节点Actor, 该方法返回的ClusterActor[]业务层不需要另行维护
     *
     * @param nodeType 节点类型, 业务层自定义
     * @return ClusterActor[] {@link ClusterActor}
     */
    public ClusterActor[] allClusterActorsOfType(Integer nodeType) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            return new ClusterActor[0];
        }
        MixedNodeActor mixedNodeActor = actorPool.getBroadcastActor();
        List<RemoteActorRef> actorRefList = mixedNodeActor.actorRefs();
        ClusterActor[] clusterActors = new ClusterActor[actorRefList.size()];
        for (int i = 0; i < actorRefList.size(); i++) {
            ActorRef actorRef = actorRefList.get(i).actorRef;
            ClusterActor clusterActor = ClusterActor.createNotBeDestroy(this, actorRef, nodeType);
            clusterActors[i] = clusterActor;
        }
        return clusterActors;
    }

    ActorRef getNewChildActor(Integer nodeType) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            return null;
        }
        if (ipGroupData.isEmpty()) {
            return randomChildActor(actorPool);
        }
        String[] localIps = localNode.getLocalIps();
        for (String localIp : localIps) {
            if (!ipGroupData.containsIp(localIp)) {
                continue;
            }
            LOGGER.debug("分组随机集群Actor, 所在组IP:{}", localIp);
            Correspondence actor = ipGroupData.randomAvailableActor(localIp, actorPool.actors());
            if (actor == null) {
                LOGGER.debug("找不到所在组IP[{}]的集群Actor, 使用通用随机分配", localIp);
                return randomChildActor(actorPool);
            }
            return actor.childActor();
        }
        return randomChildActor(actorPool);
    }

    private ActorRef newChildActorInGroup(Integer nodeType, int groupId) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            return null;
        }
        if (ipGroupData.isEmpty()) {
            return randomChildActor(actorPool);
        }
        if (groupId <= 0) { // 由系统返回的可往下执行的groupId
            return randomChildActor(actorPool);
        }
        String oneGroupIp = ipGroupData.getOneOfGroupIp(groupId);
        Correspondence actor = ipGroupData.randomAvailableActor(oneGroupIp, actorPool.actors());
        if (actor == null) {
            LOGGER.debug("找不到IP组[{}]的集群Actor, 使用通用随机分配", groupId);
            return randomChildActor(actorPool);
        }
        return actor.childActor();
    }

    private ActorRef randomChildActor(NodeActorsPool actorPool) {
        Correspondence actor = actorPool.getActor(randomStrategy, -1);
        if (actor == null) {
            return null;
        }
        return actor.childActor();
    }

    public ClusterActor getConsistentHashChildActor(Integer nodeType, int key) {
        NodeActorsPool actorPool = nodes.get(nodeType);
        if (actorPool == null) {
            throw new NullPointerException("无可用的" + nodeType + "类型节点");
        }
        Correspondence actor = actorPool.getActor(consistentHashStrategy, key);
        if (actor == null) {
            throw new NullPointerException("一致性Hash找不到可用的" + nodeType + "类型节点");
        }
        LOGGER.debug("key[{}]路由到节点Actor[{},{}]", key, nodeType, actor.connectedNodeId());
        int nodeId = actor.connectedNodeId();
        ParentActorIns actorIns = focusNodeActors.get(nodeId);
        if (actorIns == null || actorIns == PLACEHOLDER) {
            ActorRef childActor = actor.childActor();
            return ClusterActor.createWithNodeType(this, childActor, nodeType);
        }
        ActorRef childActor = actorIns.getConsistentHashFixedActor(key);
        return ClusterActor.createNotBeDestroy(this, childActor, nodeType);
    }

    public ClusterNode getLocalNode() {
        return localNode;
    }

    public EventLoopGroup getClientThreadGroup() {
        return clientThreadGroup;
    }

    public boolean isConnectedNode(Integer nodeId) {
        ParentActorIns parentActor = focusNodeActors.get(nodeId);
        if (parentActor == null) {
            return false;
        }
        return parentActor.isWorking();
    }

    /**
     * 业务层不建议使用此方法获取集群节点的Actor引用，远程节点ActorRef不作为获取途径暴露在业务层。<p>
     * 业务层维护节点类型，请使用{@link #getClusterActorByNodeType(Integer)}获取节点ClusterActor。<p>
     * 业务层维护节点ID，请使用{@link #determineClusterNodeActor(Integer)}获取节点ClusterActor。<p>
     *
     * @param nodeId 节点ID
     * @return {@link ActorRef}
     */
    ActorRef getClusterActorByNodeId(Integer nodeId) {
        ParentActorIns actorIns = focusNodeActors.get(nodeId);
        return actorIns.chooseChildActor();
    }

    /**
     * 业务层不建议使用此方法获取集群节点的Actor引用，远程节点ActorRef不作为获取途径暴露在业务层。<p>
     * 业务层维护节点类型，请使用{@link #getClusterActorByNodeType(Integer)}获取节点ClusterActor。<p>
     * 业务层维护节点ID，请使用{@link #determineClusterNodeActor(Integer)}获取节点ClusterActor。<p>
     *
     * @param nodeId 节点ID
     * @param hashCode 一致性hash码
     * @return {@link ActorRef}
     */
    ActorRef getClusterActorByNodeId(Integer nodeId, int hashCode) {
        ParentActorIns actorIns = focusNodeActors.get(nodeId);
        return actorIns.chooseChildActor(hashCode);
    }

    String _clientActorParentName() {
        return ACTOR_PARENT_NAME;
    }

    public void nodeExitsCluster(Integer nodeType, Correspondence parentActor) {
        NodeActorsPool actorsPool = nodes.get(nodeType);
        actorsPool.removeActor(parentActor);
    }

    public boolean isVoteTargetNodeFault(M2CStartFaultVote voteMessage) {
        int targetNode = voteMessage.getTargetNodeId();
        ParentActorIns actorIns = focusNodeActors.get(targetNode);
        if (actorIns == null) {
            return false;
        }
        Correspondence actor = actorIns.getCorrespondence();
        voteMessage.holdTargetNodeType(actor.connectedNodeType());
        return actor.isVoteTargetNodeFault();
    }

    void nodeFailure(NodeFaultMsg message) {
        int faultNodeId = message.getFaultNodeId();
        ParentActorIns actorIns = focusNodeActors.get(faultNodeId);
        if (actorIns == null) {
            return;
        }
        actorIns.destroyInProgress(); // 销毁进行中
        Actor parentActor = actorIns.getParentIns();
        parentActor.self().tell(message, ActorRef.noSender());
    }

    void contrastSurvivingNodesFromMaster(LinkedList<NodeData> nodesFromMaster) {
        Iterator<Map.Entry<Integer, ParentActorIns>> it = focusNodeActors.entrySet().iterator();
        for ( ; it.hasNext(); ) {
            Map.Entry<Integer, ParentActorIns> entry = it.next();
            Integer oldNodeId = entry.getKey();

            boolean existNodeAtMaster = false;
            for (NodeData nodeData : nodesFromMaster) {
                if (nodeData.getNodeId() == oldNodeId) {
                    existNodeAtMaster = true; break;
                }
            }
            if (!existNodeAtMaster) {
                // 执行节点销毁操作
                ParentActorIns parentActorIns = entry.getValue();
                CorrespondingActor actorObj = parentActorIns.getActualActor(CorrespondingActor.class);
                int nodeType = actorObj.connectedNodeType();

                LOGGER.info("检测到节点[{},{}]已退出Master集群中心, 准备移除节点", oldNodeId, nodeType);

                NodeFaultMsg nodeFaultMsg = NodeFaultMsg.create(oldNodeId, true); // 处理完子Actor的回调后强制回收Actor
                parentActorIns.getParentIns().self().tell(nodeFaultMsg, ActorRef.noSender());
                parentActorIns.destroyInProgress();
            }
        }
    }

    public void removeParentActor(Integer nodeId, @Nonnull Actor parentActor) {
        ParentActorIns actorIns = focusNodeActors.get(nodeId);
        if (actorIns == null) {
            return;
        }
        if (actorIns.getParentIns() == parentActor && actorIns.isDestroyInProgress()) {
            actorIns.destroyedCompletely();
            focusNodeActors.remove(nodeId);
        }
    }

    public ActorRef createMasterActor(MasterClient client, ProcessingCenter center) {
        int rNum = ThreadLocalRandom.current().nextInt(255);
        String actorName = MASTER_ACTOR_NAME + rNum;
        return clusterClientSys.actorOf(MClientActor.props(client, this, center), actorName);
    }

    public void registerMasterActor(MClientActor actor, int masterId, RaftState state) {
        masterActorPool.addMaster(actor, masterId, state);
    }

    MasterActorsPool getMasteActorPool() {
        return masterActorPool;
    }

    public InterconnectionStrategy getInterconnectionStrategy(Integer nodeType) {
        return nodeInterconnStrategys.get(nodeType);
    }

    public Map<Integer, InterconnectionStrategy> nodeInterconnStrategyMap() {
        return nodeInterconnStrategys;
    }

    // clientOrServer, 0-client, 1-server
    public void nodeConnectedSuccess(int nodeType, int nodeId, int clientOrServer) {
        nodeConnectedFuture.checkConnectedStateToExecute(nodeType, nodeId, clientOrServer);
    }

    public ActorSelection seekFirstConnectedActor(boolean init) {
        String actorName = "PeerNodeConnectedLocalActor";
        if (init) {
            clusterClientSys.actorOf(CorrespondingActor.props(null, this), actorName);
        }
        ActorRef userActorRef = clusterClientSys.systemImpl().guardian();
        String matchPath = userActorRef.path().toString() + "/" + actorName;
        ActorSelection selection = clusterClientSys.actorSelection(matchPath);
        return selection;
    }

    public void nodeConnectedFuture(NodeConnectedFuture future) {
        nodeConnectedFuture = future;
    }

    public NodeConnectedFuture getNodeConnectedFuture() {
        return nodeConnectedFuture;
    }

    public void resolveNodeIpGroups(List<NodeLocatedIpGroup> ipGroups) {
        if (ipGroupData == null || ipGroupData.compare(ipGroups)) {
            ipGroupData = IpGroupData.create(ipGroups);
        }
    }

    public int getIpGroupByNodeId(int nodeId) {
        if (ipGroupData.isEmpty()) {
            return -1;
        }
        if (localNode.getNodeId() == nodeId) {
            return getLocalNodeGroup();
        }
        ParentActorIns parentActorIns = focusNodeActors.get(nodeId);
        if (parentActorIns == null) {
            return -1;
        }
        CorrespondingActor parentActor = (CorrespondingActor) parentActorIns.getParentIns();
        String nodeIp = parentActor.connectedIp();
        return ipGroupData.getGroupId(nodeIp);
    }

    public int getLocalNodeGroup() {
        if (ipGroupData.isEmpty()) {
            return 0;
        }
        String[] localIps = localNode.getLocalIps();
        for (String localIp : localIps) {
            int groupId = ipGroupData.getGroupId(localIp);
            if (groupId > 0) {
                return groupId;
            }
        }
        LOGGER.info("找不到本地节点{}配置的组ID",
                Arrays.toString(localNode.getLocalIps()));
        return -1;
    }

    public List<int[]> getLocalGroupSlotRanges() {
        if (ipGroupData.isEmpty()) {
            return Collections.emptyList();
        }
        String[] localIps = localNode.getLocalIps();
        for (String localIp : localIps) {
            List<int[]> slotIds = ipGroupData.getGroupSlotRanges(localIp);
            if (!slotIds.isEmpty()) {
                return slotIds;
            }
        }
        return Collections.emptyList();
    }

    public int getIpGroup(String ip) {
        return ipGroupData.getGroupId(ip);
    }

    /**
     * 通过节点类型获取与传入ipGroup相同的IP组内的ClusterActor实例, ipGroup可以通过{@link #getIpGroupByNodeId}获取。<p>
     * 请谨慎传入IPGroup, 如果传入ipGroup不合法(与系统无关联的任意数字), 会导致报错, 后果自负。
     *
     * @param nodeType 节点类型
     * @param ipGroup IP组
     * @return {@link ClusterActor}
     */
    public ClusterActor getClusterActorInGroup(int nodeType, int ipGroup) {
        ActorRef childActor = newChildActorInGroup(nodeType, ipGroup);
        if (childActor == null) {
            throw new NullPointerException("无可用的" + nodeType + "类型节点");
        }
        return ClusterActor.createWithNodeType(this, childActor, nodeType);
    }

    public void markUsableStateOfNode(int nodeId, boolean usable) {
        ParentActorIns parentActor = focusNodeActors.get(nodeId);
        if (parentActor != null) {
            parentActor.getCorrespondence().markNodeUsable(usable);
        }
    }

    public Boolean inspectNodeIsUsable(int nodeId) {
        ParentActorIns parentActor = focusNodeActors.get(nodeId);
        if (parentActor != null) {
            return parentActor.getCorrespondence().isNodeUsable();
        }
        return null;
    }

    ActorSystem clusterSystem() {
        return clusterClientSys;
    }


    public static ClusterContext create() {
        ClusterContext clusterCtx = new ClusterContext();
        clusterCtx.initSystem();
        return clusterCtx;
    }

    public static ClusterContext create(Map<Integer, InterconnectionStrategy> strategys) {
        ClusterContext clusterCtx = create();
        clusterCtx.nodeInterconnStrategys = Collections.unmodifiableMap(strategys); // 不可变映射表
        return clusterCtx;
    }

}
