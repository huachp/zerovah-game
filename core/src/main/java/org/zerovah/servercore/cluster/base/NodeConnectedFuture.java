package org.zerovah.servercore.cluster.base;

import org.zerovah.servercore.cluster.ClusterContext;
import org.zerovah.servercore.cluster.ICallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 节点连接成功执行事件异步对象
 *
 * @author huachp
 */
public class NodeConnectedFuture extends AbstractMultiNodeFuture {
    
    private static final Logger LOGGER = LogManager.getLogger(NodeConnectedFuture.class);

    private ConcurrentMap<Integer, NodeLock> locks = new ConcurrentHashMap<>();

    private NodeConnectedFuture() {
    }

    private void lockExecute(int nodeType, int nodeId) {
        Iterator<NodeCallback> iterator = queue.iterator();
        boolean delPlaceHolder = false;
        for (; iterator.hasNext(); ) {
            NodeCallback nodeCallback = iterator.next();
            if (nodeCallback == PLACEHOLDER && delPlaceHolder) {
                iterator.remove();
                delPlaceHolder = false;
                continue;
            }
            if (!checkCallbackCanBeExecuted(nodeType, nodeCallback)) {
                continue;
            }
            if (nodeCallback.disposable) {
                LOGGER.debug("执行[{},{}]节点类型的回调", nodeType, nodeId);
                nodeCallback.callback.onCall(null);
                iterator.remove();
                delPlaceHolder = true;
            } else {
                nodeCallback.callback.onCall(nodeId);
            }
        }
    }

    // clientOrServer, 0-client, 1-server
    public void checkConnectedStateToExecute(int nodeType, int nodeId, int clientOrServer) {
        NodeLock lock = locks.get(nodeType);
        if (lock == null) {
            lock = new NodeLock();
            NodeLock old = locks.putIfAbsent(nodeType, lock);
            if (old != null) {
                lock = old;
            }
        }
        lock.safeExecute(nodeType, nodeId, clientOrServer);
    }

    public boolean isNodeInterconnected(int nodeType, int nodeId) {
        NodeLock lock = locks.get(nodeType);
        return lock != null && lock.isInterconnected(nodeId);
    }

    public boolean isServerConnected(int nodeType, int nodeId) {
        NodeLock lock = locks.get(nodeType);
        return lock != null && lock.isServerConnected(nodeId);
    }

    @Override
    public boolean executeImmediately(int nodeType, ICallback callback) {
        NodeLock lock = locks.get(nodeType);
        if (lock != null) {
            return lock.safeExecute(callback);
        }
        return false;
    }

    @Override
    public void executeIfConnectedWhenAdded(int nodeType, ICallback callback) {
        NodeLock lock = locks.get(nodeType);
        if (lock != null && !lock.inLoop) { // 事件入队时事件总线不在循环中则执行
            lock.safeExecuteAllConnected(callback);
        }
    }


    public static NodeConnectedFuture create(ClusterContext clusterCtx) {
        NodeConnectedFuture future = new NodeConnectedFuture();
        future.nodeInterconnStrategys = clusterCtx.nodeInterconnStrategyMap();
        return future;
    }


    private class NodeLock {

        private volatile boolean executed; // 首次执行状态
        private volatile boolean inLoop; // 事件循环中
        private Map<Integer, Integer> connectedStates = new HashMap<>();

        private synchronized void safeExecute(int nodeType, int nodeId, int clientOrServer) {
            if (connected(nodeId, clientOrServer)) {
                LOGGER.debug("与节点[{},{}]互连成功, 开始执行回调", nodeType, nodeId);
                inLoop = true;
                lockExecute(nodeType, nodeId);
                executed = true;
                inLoop = false;
            }
        }

        private synchronized boolean safeExecute(ICallback callback) {
            if (executed) {
                callback.onCall(null);
                return true;
            }
            return false;
        }

        private synchronized void safeExecuteAllConnected(ICallback callback) {
            for (Map.Entry<Integer, Integer> entry : connectedStates.entrySet()) {
                Integer nodeId = entry.getKey();
                Integer state = entry.getValue();
                if ((state & 0xF) == 3) {
                    callback.onCall(nodeId);
                }
            }
        }

        // clientOrServer, 0-client, 1-server
        private boolean connected(int nodeId, int clientOrServer) {
            Integer state = connectedStates.get(nodeId);
            if (state == null) {
                connectedStates.put(nodeId, state = 1 << clientOrServer);
            } else {
                connectedStates.put(nodeId, state |= (1 << clientOrServer));
            }
            return (state & 0xF) == 3;
        }

        private boolean isInterconnected(int nodeId) {
            Integer state = connectedStates.get(nodeId);
            return state != null && (state & 0xF) == 3;
        }

        private boolean isServerConnected(int nodeId) {
            Integer state = connectedStates.get(nodeId);
            return state != null && ((state >> 1) & 0xF) == 1;
        }

    }

}
