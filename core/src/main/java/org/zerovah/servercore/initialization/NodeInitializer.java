package org.zerovah.servercore.initialization;

import org.zerovah.servercore.akka.AkkaPathContext;
import org.zerovah.servercore.cluster.ClusterContext;
import org.zerovah.servercore.cluster.base.NodeConnectedFuture;
import org.zerovah.servercore.cluster.leader.NodeLeaderContext;
import org.zerovah.servercore.util.ClassContainer;

import java.util.HashMap;

/**
 * 节点初始化接口
 *
 * @author huachp
 */
public interface NodeInitializer {

    String CONNECT_STRATEGY = "node.strategy";
    String NODE_HANDLERS = "node.handlers";

    /** 主线程初始化传递参数 */
    ThreadLocal<HashMap<String, Object>> MAIN_THREAD_PARAMS = ThreadLocal.withInitial(HashMap::new);

    /**
     * 在连接集群中心(Master)之前初始化逻辑
     */
    void initBeforeConnectCluster(ClusterContext clusterCtx, AkkaPathContext akkaCtx, ClassContainer clsContainer) throws Exception;

    /**
     * 在连接成功集群中心(Master)之后处理逻辑
     */
    void doAfterConnectedCluster(ClusterContext clusterCtx, ClassContainer clsContainer) throws Exception;

    /**
     * 节点类型
     *
     * @return {@link Integer}
     */
    int nodeType();

    /**
     * 持有节点互连成功的异步处理对象
     *
     * @param future {@link NodeConnectedFuture}
     */
    default void holdConnectedFuture(NodeConnectedFuture future) {
    }

    /**
     * 初始化自身类型主从节点上下文
     */
    default void initSelfNodeLeaderCtx(NodeLeaderContext nodeLeaderCtx) {
    }

    default void collectMainParam(String key, Object value) {
        MAIN_THREAD_PARAMS.get().put(key, value);
    }

}
