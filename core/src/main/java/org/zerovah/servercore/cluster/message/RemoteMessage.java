package org.zerovah.servercore.cluster.message;

/**
 * 集群服务消息
 *
 * @author huachp
 */
public interface RemoteMessage extends Cloneable {

    /*
     * 接口说明:
     *
     * associateId(int id)
     * transformId(int id)
     * sourceNodeId(int nodeId, boolean overwrite)
     * clusterActorId(long actorId)
     *
     * 以上4个接口不应设计成开放式接口, 如果被外部调用方调用, 可能导致严重问题
     * 在未来的版本, 以上接口可能被弃用
     */

    int getId();

    RemoteMessage associateId(int id);

    RemoteMessage transformId(int id);

    long getActorId();

    RemoteMessage sourceNodeId(int nodeId);

    RemoteMessage clusterActorId(long actorId);

    int getSourceNodeId();

    int getTargetNodeType();

    int getTargetNodeId();

    int getTimeoutTime();

    RemoteMessage copy();

}
