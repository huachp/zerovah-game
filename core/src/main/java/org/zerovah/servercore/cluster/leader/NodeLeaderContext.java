package org.zerovah.servercore.cluster.leader;

import org.zerovah.servercore.cluster.ClusterContext;
import org.zerovah.servercore.cluster.ClusterNode;
import org.zerovah.servercore.cluster.actor.ClusterActor;
import org.zerovah.servercore.cluster.base.NodeConnectedFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

/**
 * Leader节点信息上下文
 *
 * @author huachp
 */
public class NodeLeaderContext {

    private static final Logger LOGGER = LogManager.getLogger(NodeLeaderContext.class);

    /** 集群上下文 */
    private ClusterContext clusterCtx;
    /** 主从上下文所属节点类型 */
    private int nodeType;
    /** 同类型其他节点的ID */
    private ConcurrentLinkedDeque<Integer> otherNodeIds = new ConcurrentLinkedDeque<>();
    /** 曾经作为Leader节点的ClusterActor */
    private ConcurrentMap<Integer, ClusterActor> leaderActors = new ConcurrentHashMap<>();
    /** 节点互连监听中状态 */
    private boolean listening;

    public NodeLeaderContext(int nodeType) {
        this.nodeType = nodeType;
    }

    public void init(ClusterContext clusterCtx) {
        if (this.clusterCtx == null) {
            this.clusterCtx = clusterCtx;
        }
    }

    public synchronized void addSelfNodeId(int selfNodeId) {
        ClusterNode localNode = clusterCtx.getLocalNode();
        if (localNode == null || localNode.getNodeId() != selfNodeId) {
            LOGGER.warn("传入的自身节点ID[{}]和{}记录的不一致", selfNodeId, localNode);
            return;
        }
        Integer firstNodeId = otherNodeIds.peek();
        if (firstNodeId == null || selfNodeId < firstNodeId) {
            otherNodeIds.offerFirst(selfNodeId);
        } else {
            addNodeIdToQueueOrderly(selfNodeId);
        }
    }

    public boolean isLeaderNode() {
        Integer firstNodeId = otherNodeIds.peek();
        return firstNodeId != null
                && clusterCtx.getLocalNode().getNodeId() == firstNodeId;
    }

    public ClusterActor getLeaderNodeActor() {
        Integer leaderNodeId = otherNodeIds.peek();
        if (clusterCtx.getLocalNode().getNodeId() == leaderNodeId) {
            throw new UnsupportedOperationException("本地节点是同类型的主节点");
        }
        return clusterCtx.determineClusterNodeActor(leaderNodeId);
    }

    public List<Integer> followerNodeIds() {
        if (otherNodeIds.size() <= 1) {
            return Collections.emptyList();
        }
        int followerCount = otherNodeIds.size() - 1;
        List<Integer> followerIds = new ArrayList<>(followerCount);

        Iterator<Integer> it = otherNodeIds.iterator();
        it.next();
        for (; it.hasNext(); ) {
            followerIds.add(it.next());
        }
        return followerIds;
    }

    private synchronized void connectedNode(int nodeId) {
        Integer firstNodeId = otherNodeIds.peek();
        if (firstNodeId == null || nodeId < firstNodeId) {
            otherNodeIds.offerFirst(nodeId);
            ClusterActor leaderNodeActor = clusterCtx.determineClusterNodeActor(nodeId);
            leaderNodeActor.failureFastNotify(data -> {
                handleLeaderNodeFault(nodeId);
            });
            ClusterActor oldLeaderActor = leaderActors.put(nodeId, leaderNodeActor);
            if (oldLeaderActor != null) {
                oldLeaderActor.destroy();
            }
        } else {
            addNodeIdToQueueOrderly(nodeId);
        }
    }

    private void addNodeIdToQueueOrderly(int nodeId) {
        Integer lastNodeId = otherNodeIds.peekLast();
        if (nodeId >= lastNodeId) {
            otherNodeIds.offer(nodeId);
        } else {
            // 来到这里说明队列里的元素不止一个
            List<Integer> readdNodeIds = new ArrayList<>();
            readdNodeIds.add(otherNodeIds.pollLast());

            Iterator<Integer> descendingIt = otherNodeIds.descendingIterator();
            for ( ; descendingIt.hasNext(); ) {
                Integer elementNodeId = descendingIt.next();
                if (nodeId >= elementNodeId) {
                    break;
                }
                readdNodeIds.add(elementNodeId);
                descendingIt.remove();
            }

            otherNodeIds.offerLast(nodeId); // 先把自己加到队列的尾部
            for (int i = readdNodeIds.size() - 1; i >= 0; i--) {
                Integer readdNodeId = readdNodeIds.get(i);
                otherNodeIds.offerLast(readdNodeId);
            }
        }
    }

    private synchronized void handleLeaderNodeFault(int nodeId) {
        Integer firstNodeId = otherNodeIds.peek(); // 取出头部
        Objects.requireNonNull(firstNodeId);
        if (nodeId != firstNodeId) {
            return; // 故障的节点不是主节点, 跳过
        }
        LOGGER.info(" Leader节点[{}]故障, 重新选取Leader节点", nodeId);
        otherNodeIds.poll();
        leaderActors.remove(nodeId);

        if (!otherNodeIds.isEmpty()) {
            int newLeaderNodeId = otherNodeIds.peek();
            int localNodeId = clusterCtx.getLocalNode().getNodeId();
            if (newLeaderNodeId == localNodeId) {
                return;
            }
            ClusterActor newLeaderActor = leaderActors.get(newLeaderNodeId);
            if (newLeaderActor == null) {
                newLeaderActor = clusterCtx.determineClusterNodeActor(newLeaderNodeId);
                newLeaderActor.failureFastNotify(data -> handleLeaderNodeFault(newLeaderNodeId));
                leaderActors.put(nodeId, newLeaderActor);
            }
        }
    }

//    public ICallback getConnectedNodeCallback() {
//        return data -> connectedNode((Integer) data);
//    }

    public void addTheSameTypeNodeConnectedListener(NodeConnectedFuture future) {
        if (!listening) {
            addSelfNodeId(clusterCtx.getLocalNode().getNodeId()); // 先加入自身节点ID
            future.addConnectedListener(nodeType, data -> connectedNode((Integer) data));
            listening = true;
        }
    }


    public static NodeLeaderContext create(int nodeType) {
        return new NodeLeaderContext(nodeType);
    }

}
