package org.zerovah.servercore.cluster;

/**
 * 集群服务器消息分发器
 *
 * @author huachp
 */
public interface MessageDistributor {


    void distribute(int nodeId, Object message);

    int checkNode(Object message);

}
