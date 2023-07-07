package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.cause.NodeException;

/**
 * 集群回调函数
 *
 * @author huachp
 */
public interface ICallback {

    int SMOOTHLY = 1;
    int EXCEPTIONALLY = -1; // 回调异常

    void onCall(Object data);

    // 超类的这个接口不是线程安全的
    // 多线程调用会出现同步赋值问题, 请谨慎调用
    default LinkedCallback exceptionally(TypedCallback<NodeException> c) {
        LinkedCallback linked = new LinkedCallback();
        linked.linkLast(this, SMOOTHLY);
        linked.linkLast(c, EXCEPTIONALLY);
        return linked;
    }

    // 这个接口不是线程安全的
    // 多线程调用会出现同步赋值问题, 请谨慎调用
    default LinkedCallback smoothly(ICallback c) {
        LinkedCallback linked = new LinkedCallback();
        linked.linkLast(this, SMOOTHLY);
        linked.linkLast(c, SMOOTHLY);
        return linked;
    }


    static ICallback ofEmpty() {
        return data -> {};
    }

    static ICallback of(ICallback nut) {
        return nut;
    }
}
