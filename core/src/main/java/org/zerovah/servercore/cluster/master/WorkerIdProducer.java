package org.zerovah.servercore.cluster.master;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * worker id 生产者
 *
 * @author huachp
 */
public class WorkerIdProducer {
    
    private static final Logger LOGGER = LogManager.getLogger(WorkerIdProducer.class);

    static final int INVALID_WORKER_ID = -1;
    static final int OCCUPIED_WORKER_ID = -2;
    static final int WORKER_ID_ALL_ASSIGNED = -3;
    static final int WORKER_ID_OUT_OF_RANGE = -4;

    // 集群运行过程中, 只要确定了workerId的配置, 不能修改
    // 在整个集群的生命周期(例如游戏从上线到下线), 服务无论维护几次, 只要确定了workerId的配置, 数据只能增加不能修改
    private List<Integer> workerIds;
    private TreeMap<Integer, NodeData> assignedWorkerIds;

    public WorkerIdProducer() {
    }

    public List<Integer> getWorkerIds() {
        return workerIds;
    }

    public Map<Integer, NodeData> getAssignedWorkerIds() {
        return assignedWorkerIds;
    }

    public int assignWorker(int initialWid, NodeData nodeData) {
        if (workerIds.isEmpty()) {
            if (initialWid <= 0) {
                return INVALID_WORKER_ID; // 传入workerId不合法
            }
            NodeData exist = assignedWorkerIds.putIfAbsent(initialWid, nodeData);
            if (exist == null || exist.getNodeId() == nodeData.getNodeId()) {
                return initialWid;
            }
            return OCCUPIED_WORKER_ID; // 传入workerId已经被其他节点占用
        } else {
            if (initialWid <= 0) {
                if (assignedWorkerIds.size() == workerIds.size()) {
                    return WORKER_ID_ALL_ASSIGNED; // workerId配置已分配完毕
                }
                int myWorkerId = findInUseWorkerId(nodeData);
                if (myWorkerId > 0) {
                    return myWorkerId;
                }
                Map.Entry<Integer, NodeData> entry = assignedWorkerIds.lastEntry();
                if (entry == null) {
                    Integer workerId = workerIds.get(0);
                    assignedWorkerIds.put(workerId, nodeData);
                    return workerId;
                } else {
                    int selectedWorkerId = findNotInUseWorkerId();
                    assignedWorkerIds.put(selectedWorkerId, nodeData);
                    return selectedWorkerId;
                }
            } else {
                if (!workerIds.contains(initialWid)) {
                    return WORKER_ID_OUT_OF_RANGE; // 传入workerId与数据库配置冲突
                }
                int myWorkerId = findInUseWorkerId(nodeData);
                if (myWorkerId > 0) {
                    if (myWorkerId != initialWid) {
                        LOGGER.info("节点[{}]所持workerId:{}与分配的workerId:{}不一致",
                                nodeData.getNodeId(), initialWid, myWorkerId);
                    }
                    return myWorkerId;
                }

                NodeData exist = assignedWorkerIds.putIfAbsent(initialWid, nodeData);
                if (exist == null || exist.getNodeId() == nodeData.getNodeId()) {
                    return initialWid;
                }
                return OCCUPIED_WORKER_ID; // 传入workerId已经被其他节点占用
            }
        }

    }

    private int findNotInUseWorkerId() {
        int selectedWorkerId = -1;
        for (Integer workerId : workerIds) {
            NodeData exist = assignedWorkerIds.get(workerId);
            if (exist == null) {
                selectedWorkerId = workerId; break;
            }
        }
        return selectedWorkerId;
    }

    private int findInUseWorkerId(NodeData nodeData) {
        int workerId = nodeData.getBindingWorkerId();
        if (assignedWorkerIds.containsKey(workerId)) {
            return workerId;
        }
        return -1;
    }

    public void recycleWorkerId(NodeData nodeData) {
        int workerId = nodeData.getBindingWorkerId();
        if (workerId > 0) {
            assignedWorkerIds.remove(workerId);
            nodeData.unbindWorkerId();
        }
    }

    void syncBindingWorkerIdNodes(List<NodeData> syncNodes) {
        for (NodeData nodeData : syncNodes) {
            int workerId = nodeData.getBindingWorkerId();
            if (workerId <= 0) {
                continue;
            }
            NodeData exist = assignedWorkerIds.putIfAbsent(workerId, nodeData);
            if (exist != null && exist.getNodeId() != nodeData.getNodeId()) {
                LOGGER.warn("同步到节点{}绑定的workerId({})与当前内存的节点{}绑定的({})不一致",
                        nodeData.getNodeId(), workerId, exist.getNodeId(), workerId);
            }
        }
    }

    void syncUnbindWorkerIdNodes(List<NodeData> syncNodes) {
        Map<Integer, NodeData> syncNodeMap = new HashMap<>();
        for (NodeData nodeData : syncNodes) {
            syncNodeMap.put(nodeData.getNodeId(), nodeData); // 换成映射关系
        }
        Iterator<NodeData> it = assignedWorkerIds.values().iterator();
        for ( ; it.hasNext(); ) {
            if (syncNodeMap.get(it.next().getNodeId()) == null) {
                it.remove();
            }
        }
    }


    public static WorkerIdProducer create(List<Integer> workerIds) {
        WorkerIdProducer producer = new WorkerIdProducer();
        producer.workerIds = new ArrayList<>();
        producer.assignedWorkerIds = new TreeMap<>();
        boolean illegal = false;
        for (Integer incomingWorkerId : workerIds) {
            if (incomingWorkerId <= 0) {
                illegal = true; continue;
            }
            if (producer.workerIds.contains(incomingWorkerId)) {
                illegal = true; continue;
            }
            producer.workerIds.add(incomingWorkerId);
        }
        if (illegal) {
            LOGGER.warn("传入的workerId列表存在非法的数据: {}", workerIds);
        }
        return producer;
    }

    public static WorkerIdProducer createBackup() {
        WorkerIdProducer producer = new WorkerIdProducer();
        // workerIds is null
        producer.assignedWorkerIds = new TreeMap<>();
        return producer;
    }

}
