package org.zerovah.servercore.cluster.master;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.zerovah.servercore.cluster.NodeLocatedIpGroup;
import org.zerovah.servercore.cluster.actor.MServerActor;
import org.zerovah.servercore.cluster.actor.MasterFollowerActor;
import org.zerovah.servercore.cluster.cause.NodeException;
import org.zerovah.servercore.cluster.message.*;
import io.netty.channel.Channel;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master节点管理上下文
 *
 * @author huachp
 */
public class MasterContext {

    private static final Logger LOGGER = LogManager.getLogger(MasterContext.class);

    private static final String MASTER_AKKA_SYSTEM_NAME = "master-system";
    private static final String MASTER_MESSAGE_ACTOR = "master-message-actor";
    private static final String MASTER_FOLLOWER_ACTOR = "master-follower-actor";

    public static final int INITIAL_ID_SHIFT = 8; // 初始ID移位
    public static final int INITIAL_ID_BITS = 4; // 初始ID占位
    private static final int PORT_RANGE = 300; // 端口范围
//    private static final int MAX_PORT_INCREMENT = 1000; // 最大端口增量

    private int masterId;

    private ActorSystem masterActorSys;

    private int initialPort; // 初始端口
    private AtomicInteger portBuilder;

    private int initialId; // 初始ID
    private AtomicInteger nodeIdBuilder = new AtomicInteger();

    /** 注册节点信息{节点ID=注册节点数据} */
    private ConcurrentMap<Integer, RegisteredNode> registerNodes = new ConcurrentHashMap<>();
    /** 故障节点投票结果{节点ID=故障投票数据} */
    private ConcurrentMap<Integer, VoteResults> nodeFaultVotes = new ConcurrentHashMap<>();
    /** 节点IP组配置, 由DB节点传入, 集群运行过程中, 节点IP配置只能增加不能删除不能修改 */
    private ArrayList<NodeLocatedIpGroup> nodeIpGroupConfigs = new ArrayList<>();
    /** 节点固定workerId生产者 */
    private WorkerIdProducer workerIdProducer;
    /** Master主从逻辑处理Actor */
    private ActorRef mfClientActorRef;
    /** 已退出集群的节点数据 */
    private final Cache<Integer, RegisteredNode> exitedNodes = Caffeine.newBuilder()
            .maximumSize(500) // 只缓存500个节点信息, 如果有500个节点
            .removalListener((key, value, cause) -> {
                if (cause == RemovalCause.SIZE) {
                    LOGGER.info("删除已退出集群的节点注册数据, 节点ID:{}", key);
                }
            }).build();


    public MasterContext() {
        initSystem();
    }

    public MasterContext(int masterId) {
        this();
        this.masterId = masterId;
    }

    private void initSystem() {
        masterActorSys = ActorSystem.create(MASTER_AKKA_SYSTEM_NAME);
    }

    public int getMasterId() {
        return masterId;
    }

    // 这个方法线程不安全的, 要确保调用者是串行执行的
    private int allocateNodeId() {
        int increment = nodeIdBuilder.incrementAndGet();
        int max = (1 << INITIAL_ID_SHIFT) - 1;
        if (increment > max) {
            initialId ++;
            nodeIdBuilder.set(0);
            increment = 0;
            if (initialId > (1 << INITIAL_ID_BITS) - 1) {
                initialId = 0;
                LOGGER.info("initial id 重置成初始状态, 请关注节点ID是否会存在冲突");
            }
        }
        int nodeId = (initialId << INITIAL_ID_SHIFT) | increment;
        if (registerNodes.containsKey(nodeId)) {
            LOGGER.warn("生成节点ID存在重复数据[{}], 尝试重新生成", nodeId);
            nodeId = allocateNodeId();
        }
        return nodeId;
    }

    private int allocatePort() {
        int nextPort = portBuilder.incrementAndGet();
        if (nextPort > initialPort + PORT_RANGE) {
            portBuilder.compareAndSet(portBuilder.get(), initialPort);
            nextPort = portBuilder.incrementAndGet();
        }
        return nextPort;
    }

    public void requestMasterFollowersAddress(C2MLeaderFollowerServers reqMessage) {
        String serversAddrInfo = SystemPropertyUtil.get("master.servers", "");
        M2CLeaderFollowerServers serversInfoResp = M2CLeaderFollowerServers.create(masterId);
        if (StringUtils.isBlank(serversAddrInfo)) {
            int port = SystemPropertyUtil.getInt("master.port", -1);
            serversInfoResp.fillAddressInfo(masterId, new InetSocketAddress(port));
        } else {
            String[] addressInfos = StringUtils.split(serversAddrInfo, '\n');
            for (String addressInfo : addressInfos) {
                String[] idAndAddr = StringUtils.split(addressInfo, ':');
                int masterId = Integer.parseInt(idAndAddr[0]);
                String ip = idAndAddr[1];
                int port = Integer.parseInt(idAndAddr[2]);
                serversInfoResp.fillAddressInfo(masterId, new InetSocketAddress(ip, port));
            }
        }
        reqMessage.getChannel().writeAndFlush(serversInfoResp);
    }

    public void initNode(C2MInitNodeMsg initNodeMsg) {
        if (initNodeMsg.getExistingNodeId() > 0) { // 如果节点已经分配好ID和端口
            registerNodeAgain(initNodeMsg); return;
        }
        int nodeId = allocateNodeId(); // 生成节点ID
        int port = allocatePort(); // 分配端口
        int nodeType = initNodeMsg.getNodeType();
        String[] addresses = initNodeMsg.getAddresses();

        ArrayList<Integer> workerIds = initNodeMsg.getWorkerIdList();
        submitAndInitWorkerIdProducer(workerIds); // 提交并初始workerId配置
        ArrayList<NodeLocatedIpGroup> ipGroups = initNodeMsg.getNodeIpList();
        boolean broadcastUpdate = updateNodeIpGroupsConfig(ipGroups);

        NodeData nodeData = NodeData.create(nodeId, nodeType, addresses, port);
        RegisteredNode node = RegisteredNode.create(nodeData, broadcastUpdate);
        registerNodes.put(nodeId, node);

        Channel channel = initNodeMsg.getChannel();
        M2CInitNodeMsg initNodeResp = M2CInitNodeMsg.create(masterId, RaftState.LEADER);
        channel.writeAndFlush(initNodeResp.attach(nodeId, nodeType, port));
    }

    private void registerNodeAgain(C2MInitNodeMsg initNodeMsg) {
        int nodeType = initNodeMsg.getNodeType();
        String[] addresses = initNodeMsg.getAddresses();
        int nodeId = initNodeMsg.getExistingNodeId();
        int port = initNodeMsg.getExistingPort(); // 节点已绑定端口

        submitAndInitWorkerIdProducer(initNodeMsg.getWorkerIdList());
        RegisteredNode node = registerNodes.get(nodeId);
        if (node == null) {
            node = exitedNodes.getIfPresent(nodeId);
            if (node != null) {
                exitedNodes.invalidate(nodeId);
                registerNodes.put(nodeId, node);
                M2CInitNodeMsg initNodeResp = M2CInitNodeMsg.create(masterId, RaftState.LEADER);
                initNodeMsg.getChannel().writeAndFlush(initNodeResp.attach(nodeId, nodeType, -1));
            } else {
                updateNodeIpGroupsConfig(initNodeMsg.getNodeIpList());

                NodeData nodeData = NodeData.create(nodeId, nodeType, addresses, port);
                node = RegisteredNode.create(nodeData);
                registerNodes.put(nodeId, node);
                node.channel(initNodeMsg.getChannel());

                broadcastNodeRegister(node); // 重新注册广播
            }
        } else {
            NodeData registeredNodeData = node.getNodeData();
            if (registeredNodeData.exactMatch(nodeType, addresses, port)) {
                M2CNodeMsg nodeResp = M2CNodeMsg.create(node.getNodeData());
                nodeResp.nodeJoin();
                initNodeMsg.getChannel().writeAndFlush(nodeResp);
                node.channel(initNodeMsg.getChannel()); // 绑定channel
            } else {  // 节点ID冲突处理
                M2CInitNodeMsg initNodeResp = M2CInitNodeMsg.create(masterId, RaftState.FOLLOWER);
                initNodeMsg.getChannel().writeAndFlush(initNodeResp.dataConflit(nodeId));
            }
        }
    }

    public void registerNode(C2MRegisterNodeMsg registerNodeMsg) {
        int nodeId = registerNodeMsg.getNodeId();
        RegisteredNode node = registerNodes.get(nodeId);
        if (node == null) {
            LOGGER.warn("注册节点找不到节点保留信息, 节点ID:{}", nodeId);
            return;
        }
        if (registerNodeMsg.isPortAlreadyBound()) {
            Channel channel = registerNodeMsg.getChannel();
            node.channel(channel); // 绑定channel

            M2CNodeListMsg nodeListMsg = createRegisteredNodesMsg();
            channel.writeAndFlush(nodeListMsg);
            broadcastNodeRegister(node);
            broadcastNodesSync();
        } else {
            NodeData nodeData = node.getNodeData();
            Channel channel = registerNodeMsg.getChannel();
            int port = allocatePort(); // 分配端口
            nodeData.newPort(port);
            M2CInitNodeMsg initNodeResp = M2CInitNodeMsg.create(masterId, RaftState.LEADER);
            channel.writeAndFlush(initNodeResp.attach(nodeId, nodeData.getNodeType(), nodeData.getPort()));
        }
    }

    private void broadcastNodeRegister(RegisteredNode newNode) {
        M2CNodeMsg nodeResp = M2CNodeMsg.create(newNode.getNodeData());
        nodeResp.nodeJoin();
        if (newNode.isUpdateIpGroup()) {
            nodeResp.syncIpGroups(nodeIpGroupConfigs);
        }
        broadcastNodes(nodeResp);
    }

    private void broadcastNodes(Object nodeResp) {
        Iterator<RegisteredNode> iterator = registeredNodesValues().iterator();
        for ( ; iterator.hasNext(); ) {
            RegisteredNode node = iterator.next();
            if (node.isConnected()) {
                node.getChannel().writeAndFlush(nodeResp);
            } else if (node.isInvalid()) {
                iterator.remove();
                exitClusterToRecycleWorkerId(node.getNodeData());
                LOGGER.info("移除超时不可用的节点信息:{}", node.getNodeData());
            }
        }
    }

    private M2CNodeListMsg createRegisteredNodesMsg() {
        M2CNodeListMsg nodeListMsg = M2CNodeListMsg.create(nodeIpGroupConfigs);
        for (RegisteredNode node : registerNodes.values()) {
            if (node.isConnected()) {
                nodeListMsg.addNode(node.getNodeData());
            }
        }
        return nodeListMsg;
    }

    public void initiateFaultElection(C2MInitiateFaultElection msg) {
        int intiatorNode = msg.getLocalNodeId();
        int targetNode = msg.getFaultNodeId();

        RegisteredNode targetNodeData = registerNodes.get(targetNode);
        if (targetNodeData == null) {
            LOGGER.info("节点[{}]可能已经退出集群, 不必再发起故障投票, 发起方节点[{}]",
                    targetNode, intiatorNode);
            msg.getChannel().writeAndFlush(NodeFaultMsg.create(targetNode)); // 回复故障
            return;
        }
        boolean isBroadcastVote = initVoteResults(intiatorNode, targetNode);
        if (isBroadcastVote) {
            M2CStartFaultVote startFaultVote = M2CStartFaultVote.create(targetNode);
            broadcastStartVote(startFaultVote);
            LOGGER.info("节点{}可能存在故障问题, 由节点{}发起故障投票",
                    registerNodes.get(targetNode), registerNodes.get(intiatorNode));
        }

    }

    private boolean initVoteResults(int intiatorNode, int targetNode) {
        if (registerNodes.size() <= 1) { // 只有一个注册节点, 跳过
            return false;
        }
        VoteResults voteResults = nodeFaultVotes.get(targetNode);
        int expectedCount = Math.max(availableNodes() / 2, 1);
        if (voteResults == null) {
            voteResults = VoteResults.create(targetNode, expectedCount);
            VoteResults exist = nodeFaultVotes.putIfAbsent(targetNode, voteResults);
            if (exist != null) {
                exist.increaseInitiatorCount(intiatorNode);
            }
        } else {
            voteResults.increaseInitiatorCount(intiatorNode);
        }
        return voteResults.isBroadcastVote();
    }

    private void broadcastStartVote(M2CStartFaultVote startVoteMsg) {
        broadcastNodes(startVoteMsg);
    }

    private int availableNodes() {
        int count = 0;
        for (RegisteredNode elementNode : registerNodes.values()) {
            if (elementNode.isConnected()) {
                count ++;
            }
        }
        return count;
    }

    public void nodeFaultVote(C2MNodeFaultVote msg) {
        int voteNode = msg.getLocalNodeId();
        int faultNode = msg.getFaultNodeId();
        VoteResults voteResults = nodeFaultVotes.get(faultNode);
        if (voteResults == null) {
            LOGGER.warn("节点[{}]的故障投票还未发起", faultNode);
            return;
        }
        LOGGER.info("收到节点{}的投票, 故障节点{}",
                registerNodes.get(voteNode), registerNodes.get(faultNode));

        voteResults.increaseVotes(voteNode);

        LOGGER.info("投票数量:{}", voteResults);
        if (voteResults.isVotePassed()) {
            if (voteResults.isNotifiedNodes()) {
                return;
            }
            NodeFaultMsg nodeFault = NodeFaultMsg.create(voteResults.getNodeId());
            broadcastNodeFault(nodeFault);
            voteResults.nodeFaultAndNoitiedAllNodes();
            nodeFaultVotes.remove(faultNode);

            RegisteredNode registeredNode = registerNodes.remove(faultNode);
            registeredNode.closeChannel();
            exitedNodes.put(faultNode, registeredNode); // 缓存已退出集群的节点

            exitClusterToRecycleWorkerId(registeredNode.getNodeData()); // 回收workerId
            broadcastNodesSync();
            LOGGER.info("节点{}故障投票结果达到预期, 判定故障, 通知所有集群节点处理", registeredNode);
        }
    }

    private void broadcastNodeFault(NodeFaultMsg nodeFaultMsg) {
        broadcastNodes(nodeFaultMsg);
    }

    public void switchoverMasterVote(C2MSwitchoverMaster switchoverMsg) {
        mfClientActorRef.tell(LeaderFollowerMsg.create(switchoverMsg), ActorRef.noSender());
    }

    public Collection<RegisteredNode> registeredNodesValues() {
        return registerNodes.values();
    }

    public ActorSystem system() {
        return masterActorSys;
    }

    public ActorRef createServerActor() {
        String actorName = MASTER_MESSAGE_ACTOR + 0;
        return masterActorSys.actorOf(MServerActor.props(this), actorName);
    }

    public void syncRegisteredNodes(List<NodeData> nodeList) {
        registerNodes.clear();
        if (!nodeList.isEmpty()) {
            int maxNodeId = 0;
            for (NodeData nodeData : nodeList) {
                RegisteredNode registeredNode = RegisteredNode.create(nodeData);
                int nodeId = nodeData.getNodeId();
                maxNodeId = Math.max(maxNodeId, nodeId);
                registerNodes.put(nodeId, registeredNode);
            }
            initialId = maxNodeId >> INITIAL_ID_SHIFT;
            int idSuffix = maxNodeId & ((1 << INITIAL_ID_SHIFT) - 1);
            nodeIdBuilder.set(idSuffix);

            int maxPort = initialPort + maxNodeId;
            if (initialPort / 1000 == maxPort / 1000) {
                portBuilder.set(maxPort);
            }
        }
        submitSyncWorkerIdProducer(nodeList); // 同步workerId绑定关系
        LOGGER.info("同步初始高位ID:{}, 低位ID:{}, 计算出分配端口号:{}, 可用节点信息:{}",
                initialId, nodeIdBuilder.get(), portBuilder.get(), nodeList);
    }

    public void continuedNodeAllocationInit(int maxNodeId, int maxPort) {
        int frontId = maxNodeId >> INITIAL_ID_SHIFT;
        int increment = maxNodeId & ((1 << INITIAL_ID_SHIFT) - 1);
        if (frontId > initialId) {
            initialId = frontId;
        } else if (frontId == initialId) {
            nodeIdBuilder.set(Math.max(increment, nodeIdBuilder.get()));
        }
        portBuilder.set(Math.max(maxPort, portBuilder.get()));
        LOGGER.info("同步到节点ID头:{}, 节点ID尾:{}, 端口:{}",
                initialId, nodeIdBuilder.get(), portBuilder.get());
    }

    public void broadcastNodesSync() {
        SyncAvailableNodes syncNodes = SyncAvailableNodes.create(masterId);
        for (RegisteredNode registeredNode : registeredNodesValues()) {
            if (registeredNode.isConnected()) {
                syncNodes.addNodeData(registeredNode.getNodeData());
            }
        }
        mfClientActorRef.tell(syncNodes, ActorRef.noSender());
    }

    // 返回是否要触发广播更新
    private boolean updateNodeIpGroupsConfig(ArrayList<NodeLocatedIpGroup> ipGroups) {
        if (ipGroups.isEmpty()) {
            return false;
        }
        if (ipGroups.size() > this.nodeIpGroupConfigs.size()) {
            LOGGER.info("更新节点IP分组配置:{}", ipGroups);
            this.nodeIpGroupConfigs = ipGroups;
            return true;
        }
        return false;
    }

    private void submitAndInitWorkerIdProducer(List<Integer> workerIds) {
        if (workerIds == null) {
            return;
        }
        if (this.workerIdProducer == null) {
            LOGGER.info("初始固定workerId配置数据:{}", workerIds);
            this.workerIdProducer = WorkerIdProducer.create(workerIds);
        } else if (this.workerIdProducer.getWorkerIds() == null) {
            // 备份对象
            Collection<NodeData> nodes = this.workerIdProducer.getAssignedWorkerIds().values();
            this.workerIdProducer = WorkerIdProducer.create(workerIds);
            this.workerIdProducer.syncBindingWorkerIdNodes(new ArrayList<>(nodes));
        }
    }

    private void submitSyncWorkerIdProducer(List<NodeData> nodeList) {
        boolean syncRightNow = false;
        for (NodeData nodeData : nodeList) {
            int workerId = nodeData.getBindingWorkerId();
            if (workerId > 0) {
                syncRightNow = true;
            }
        }
        if (syncRightNow) {
            if (this.workerIdProducer == null) {
                LOGGER.info("从Master可用服务同步备份WorkerIdProducer");
                this.workerIdProducer = WorkerIdProducer.createBackup();
            }
            this.workerIdProducer.syncBindingWorkerIdNodes(nodeList);
        }
        if (this.workerIdProducer != null) {
            this.workerIdProducer.syncUnbindWorkerIdNodes(nodeList);
        }
    }

    public void assignWorkerIdToNode(C2MApplyForWorkerIdMsg applyMsg) {
        int initialWid = applyMsg.getInitailWorkerId();
        int nodeId = applyMsg.getNodeId();
        RegisteredNode node = registerNodes.get(nodeId);
        if (node == null) {
            LOGGER.warn("节点[{}]未注册集群, 不能申请workerId", nodeId);
            NodeException ex = NodeException.remoteFailure("未注册集群, 不能申请workerId");
            CallbackMessage callbackMsg = SimpleCallbackMsg.create(ex)
                    .linkCallback(applyMsg.getCallbackId());
            applyMsg.getChannel().writeAndFlush(callbackMsg);
            return;
        }
        if (workerIdProducer == null) {
            LOGGER.warn("workerIdProducer未初始化, 请检查逻辑");
            NodeException ex = NodeException.remoteFailure("申请workerId请求必须在DB节点连上集群后");
            CallbackMessage callbackMsg = SimpleCallbackMsg.create(ex)
                    .linkCallback(applyMsg.getCallbackId());
            applyMsg.getChannel().writeAndFlush(callbackMsg);
            return;
        }
        NodeData nodeData = node.getNodeData();
        int workerId = workerIdProducer.assignWorker(initialWid, nodeData);
        if (workerId > 0) {
            nodeData.bindWorkerId(workerId);
            broadcastNodesSync(); // 同步一次节点信息
        }
        CallbackMessage callbackMsg = SimpleCallbackMsg.create(workerId)
                .linkCallback(applyMsg.getCallbackId());
        applyMsg.getChannel().writeAndFlush(callbackMsg);

    }

    private void exitClusterToRecycleWorkerId(NodeData nodeData) {
        if (workerIdProducer == null) {
            LOGGER.info("WorkerIdProducer对象未初始化"); return;
        }
        workerIdProducer.recycleWorkerId(nodeData);
    }

    public void checkNodeConnections() {
        for (RegisteredNode elementNode : registerNodes.values()) {
            if (elementNode.getChannel() != null
                    && !elementNode.getChannel().isActive()) { // 不为空且断开
                elementNode.disconnect();
            }
        }
    }

    public ActorRef getMfClientActorRef() {
        return mfClientActorRef;
    }

    private void initFollowerActor(TreeMap<Integer, MasterFollowerClient> clients) {
        Props props = MasterFollowerActor.props(masterId, clients);
        String actorName = MASTER_FOLLOWER_ACTOR + 0;
        mfClientActorRef = masterActorSys.actorOf(props, actorName);
    }


    public static MasterContext create(int masterId, TreeMap<Integer, MasterFollowerClient> clients) {
        if (masterId < 0) {
            throw new IllegalArgumentException("MASTER ID小于0 : " + masterId);
        }
        int initialPort = SystemPropertyUtil.getInt("node.initial.port", -1000);
        if (initialPort < 0) {
            throw new IllegalArgumentException("初始分配端口小于0, 配置端口: " + initialPort);
        }
        List<Integer> masterIds = new LinkedList<>();
        masterIds.add(masterId);
        masterIds.addAll(clients.keySet());
        Collections.sort(masterIds);
        int index = masterIds.indexOf(masterId);
        initialPort += index * PORT_RANGE;

        MasterContext masterCtx = new MasterContext(masterId);
        masterCtx.initialPort = initialPort;
        masterCtx.portBuilder = new AtomicInteger(initialPort);
        masterCtx.initFollowerActor(clients);
        LOGGER.info("创建Master上下文, 初始端口: {}", masterCtx.portBuilder.get());
        return masterCtx;
    }


}
