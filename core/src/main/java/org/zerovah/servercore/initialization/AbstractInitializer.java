package org.zerovah.servercore.initialization;

import org.zerovah.servercore.cluster.ICallback;
import org.zerovah.servercore.cluster.base.NodeConnectedFuture;
import org.zerovah.servercore.cluster.leader.NodeLeaderContext;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * 抽象节点初始化器
 *
 * @author huachp
 */
public abstract class AbstractInitializer implements NodeInitializer {

    private NodeLeaderContext selfNodeLeaderCtx;

    private NodeConnectedFuture nodeConnectedFuture;

    private LinkedHashMap<Integer, ICallback> firstConnectedNodeCallbacks = new LinkedHashMap<>();

    private LinkedHashMap<Integer, ICallback> connectedNodeCallbacks = new LinkedHashMap<>();

    public void holdConnectedFuture(NodeConnectedFuture future) {
        nodeConnectedFuture = future;
        Iterator<Entry<Integer, ICallback>> it = firstConnectedNodeCallbacks.entrySet().iterator();
        for ( ; it.hasNext(); ) {
            Entry<Integer, ICallback> entry = it.next();
            nodeConnectedFuture.addFirstConnectedListener(entry.getKey(), entry.getValue());
            it.remove();
        }
        for (it = connectedNodeCallbacks.entrySet().iterator(); it.hasNext(); ) {
            Entry<Integer, ICallback> entry = it.next();
            nodeConnectedFuture.addConnectedListener(entry.getKey(), entry.getValue());
            it.remove();
        }
    }

    public void initSelfNodeLeaderCtx(NodeLeaderContext leaderCtx) {
        this.selfNodeLeaderCtx = leaderCtx;
    }

    /**
     * 该方法注册的回调, 在成功连接上对应类型的节点后只执行一次, 回调执行完成销毁回调对象
     *
     * @param nodeType
     * @param callback
     */
    public void addFirstConnectedNodeCallback(int nodeType, ICallback callback) {
        if (nodeConnectedFuture != null) {
            nodeConnectedFuture.addFirstConnectedListener(nodeType, callback);
        } else {
            firstConnectedNodeCallbacks.put(nodeType, callback);
        }
    }

    /**
     * 该方法注册的回调, 在每次成功连接上对应类型的节点后执行
     *
     * @param nodeType
     * @param callback
     */
    public void addConnectedNodeCallback(int nodeType, ICallback callback) {
        if (nodeConnectedFuture != null) {
            nodeConnectedFuture.addConnectedListener(nodeType, callback);
        } else {
            connectedNodeCallbacks.put(nodeType, callback);
        }
    }

    public NodeLeaderContext getMyNodeLeaderCtx() {
        return selfNodeLeaderCtx;
    }

}
