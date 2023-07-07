package org.zerovah.servercore.cluster.base;

import org.zerovah.servercore.cluster.ICallback;
import org.zerovah.servercore.cluster.INodeFuture;
import org.zerovah.servercore.cluster.InterconnectionStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 多节点逻辑操作成功后异步调用执行对象(-_-!)
 *
 * @author huachp
 */
public abstract class AbstractMultiNodeFuture implements INodeFuture {

    static final NodeCallback PLACEHOLDER = new NodeCallback(-100, null);

    protected ConcurrentLinkedQueue<NodeCallback> queue = new ConcurrentLinkedQueue<>();

    protected Map<Integer, InterconnectionStrategy> nodeInterconnStrategys;

    @Override
    public INodeFuture addConnectedListener(int nodeType, ICallback callback) {
        executeIfConnectedWhenAdded(nodeType, callback);
        enqueue(new NodeCallback(nodeType, callback));
        return this;
    }

    @Override
    public INodeFuture addListener(ICallback callback) {
        throw new UnsupportedOperationException();
    }

    public INodeFuture addFirstConnectedListener(int nodeType, ICallback callback) {
        boolean executed = executeImmediately(nodeType, callback);
        if (!executed) {
            enqueue(new NodeCallback(nodeType, callback).disposable());
        }
        return this;
    }

    public INodeFuture addFirstConnectedListener(NodeMainType nodeMainType, ICallback callback) {
        boolean executed = false;
        for (Map.Entry<Integer, InterconnectionStrategy> entry : nodeInterconnStrategys.entrySet()) {
            Integer nodeType = entry.getKey();
            InterconnectionStrategy strategy = entry.getValue();
            if (strategy.nodeType() == nodeMainType) {
                executed = executeImmediately(nodeType, callback);
            }
        }
        if (!executed) {
            enqueue(new NodeCallback(nodeMainType, callback).disposable());
        }
        return this;
    }

    protected boolean checkCallbackCanBeExecuted(int nodeType, NodeCallback nodeCallback) {
        if (nodeCallback.nodeType >= 0) {
            return nodeType == nodeCallback.nodeType;
        }
        if (nodeCallback.mainType != null) {
            InterconnectionStrategy interconnStrategy = nodeInterconnStrategys.get(nodeType);
            if (interconnStrategy != null) {
                return interconnStrategy.nodeType() == nodeCallback.mainType;
            }
        }
        return false;
    }

    protected synchronized void enqueue(NodeCallback element) {
        queue.offer(element);
        queue.offer(PLACEHOLDER);
    }

    public abstract boolean executeImmediately(int nodeType, ICallback callback);

    public abstract void executeIfConnectedWhenAdded(int nodeType, ICallback callback);


    static class NodeCallback {

        int nodeType = -1; // 节点类型(业务层)
        NodeMainType mainType; // 节点大类
        ICallback callback;
        boolean disposable; // 一次性

        NodeCallback(int nodeType, ICallback callback) {
            this.nodeType = nodeType;
            this.callback = callback;
        }

        NodeCallback(NodeMainType nodeMainType, ICallback callback) {
            this.mainType = nodeMainType;
            this.callback = callback;
        }

        NodeCallback disposable() {
            this.disposable = true;
            return this;
        }

    }

}
