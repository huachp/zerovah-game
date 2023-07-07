package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.actor.FaultDetectionRecord;

/**
 * 节点故障消息通知
 *
 * @author huachp
 */
public class NodeFaultMsg {

    private static final int HANDLED_BY_PARENT_NODE = 1;
    private static final int HANDLED_BY_CHILD_NODE = 2;

    /** 故障节点ID */
    private int faultNodeId;
    /** 消息接收后处理的Actor节点对象 */
    private int handledActor;
    /** 故障检测记录, 由父节点Actor注入, 广播到所有子节点Actor处理 */
    private FaultDetectionRecord record;
    /** 强制回收节点父Actor */
    private boolean forceRecycle;

    public NodeFaultMsg() {
    }

    public int getFaultNodeId() {
        return faultNodeId;
    }

    public boolean isHandledByParentActor() {
        return handledActor == HANDLED_BY_PARENT_NODE;
    }

    public boolean isHandledByChildActor() {
        return handledActor == HANDLED_BY_CHILD_NODE;
    }

    public void thenHandledByChildActor() {
        handledActor = HANDLED_BY_CHILD_NODE;
    }

    public boolean isForceRecycle() {
        return forceRecycle;
    }

    public FaultDetectionRecord getRecord() {
        return record;
    }

    public NodeFaultMsg detectionRecord(FaultDetectionRecord record) {
        this.record = record;
        return this;
    }


    public static NodeFaultMsg create(int faultNodeId) {
        NodeFaultMsg msg = new NodeFaultMsg();
        msg.faultNodeId = faultNodeId;
        msg.handledActor = HANDLED_BY_PARENT_NODE; // 由父节点Actor优先处理
        return msg;
    }

    public static NodeFaultMsg create(int faultNodeId, boolean forceRecycle) {
        NodeFaultMsg msg = new NodeFaultMsg();
        msg.faultNodeId = faultNodeId;
        msg.forceRecycle = forceRecycle;
        msg.handledActor = HANDLED_BY_PARENT_NODE; // 由父节点Actor优先处理
        return msg;
    }
}
