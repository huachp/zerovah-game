package org.zerovah.servercore.cluster.message;

import akka.actor.ActorRef;
import org.zerovah.servercore.cluster.ClusterNode;


/**
 * 抽象集群服务消息
 *
 * @author huachp
 */
public abstract class AbstractRemoteMsg implements RemoteMessage {

    /** 消息ID */
    protected int messageId = -1;
    /** actor ID */
    protected long actorId;
    /** 源节点ID */
    protected int srcNodeId;
    /** 目标节点类型 */
    protected int targetNodeType = -1;
    /** 目标节点ID */
    protected int targetNodeId;
    /** 处理消息的Actor引用 */
    private transient ActorRef actorRef; // 不序列化
    /** 超时时间秒 */
    private transient int timeoutTime = 10;


    protected AbstractRemoteMsg() {
    }

    @Override
    public int getId() {
        return messageId;
    }

    @Override
    public long getActorId() {
        return actorId;
    }

    @Override
    public int getSourceNodeId() {
        return srcNodeId;
    }

    @Override
    public AbstractRemoteMsg associateId(int id) {
        if (this.messageId > 0) { // 消息ID必须唯一, 不能变更
            return this;
        }
        this.messageId = id;
        return this;
    }

    @Override
    public RemoteMessage transformId(int id) {
        this.messageId = id;
        return this;
    }

    public ActorRef workingActor() {
        return actorRef;
    }

    /**
     * 注意, 此处传入的ActorRef实体必须是{@link com.janlr.ag.core.cluster.actor.BaseMessageActor}的继承类; <p>
     * 不然, 务必在传入的自定义ActorRef实体内显式对远程回调消息{@link CallbackMessage}进行处理; <p>
     *
     * @param actorRef  传入的Actor引用对象
     * @return this
     */
    public AbstractRemoteMsg actor(ActorRef actorRef) {
        if (this.actorRef != null) { // 消息处理Actor不能变更
            return this;
        }
        this.actorRef = actorRef;
        return this;
    }

    @Override
    public AbstractRemoteMsg clusterActorId(long actorId) {
        long lowBitId = actorId & Integer.MAX_VALUE;
        if (this.actorId <= 0) {
            this.actorId = actorId;
        } else if (this.actorId == lowBitId) {
            this.actorId = actorId;
        } else {
//            throw new IllegalArgumentException("传入actorId格式不正确");
        }
        return this;
    }

    public AbstractRemoteMsg sourceNodeId(ClusterNode clusterNode) {
        if (this.srcNodeId <= 0) {
            this.srcNodeId = clusterNode.getNodeId();
        } else if (clusterNode.getNodeId() != this.srcNodeId) {
//            throw new IllegalArgumentException("传入节点ID与持有节点ID不一致");
        }
        return this;
    }

    @Override
    public AbstractRemoteMsg sourceNodeId(int nodeId) {
        this.srcNodeId = nodeId;
        return this;
    }

    @Override
    public int getTargetNodeType() {
        return targetNodeType;
    }

    @Override
    public int getTargetNodeId() {
        return targetNodeId;
    }

    public void determinedNodeType(int nodeType) {
        this.targetNodeType = nodeType;
    }

    public void determinedNodeId(int nodeId) {
        this.targetNodeId = nodeId;
    }

    public AbstractRemoteMsg timeoutIn5To10Seconds() {
        this.timeoutTime = 5;
        return this;
    }

    public AbstractRemoteMsg timeout(int seconds) {
        this.timeoutTime = Math.min(seconds, 50);
        return this;
    }

    @Override
    public int getTimeoutTime() {
        return timeoutTime;
    }

    @Override
    public RemoteMessage copy() {
        try {
            return (RemoteMessage) super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName())
                .append('{')
                .append("messageId=").append(messageId)
                .append(", clientActorId=").append(actorId)
                .append(", srcNodeId=").append(srcNodeId)
                .append('}');
        return builder.toString();
    }

}
