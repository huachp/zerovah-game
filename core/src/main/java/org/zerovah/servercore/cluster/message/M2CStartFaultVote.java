package org.zerovah.servercore.cluster.message;

/**
 * 开始故障投票
 *
 * @author huachp
 */
public class M2CStartFaultVote {

    private int targetNodeId;

    private transient int targetNodeType;

    public M2CStartFaultVote() {
    }

    public int getTargetNodeId() {
        return targetNodeId;
    }

    public int getTargetNodeType() {
        return targetNodeType;
    }

    public void holdTargetNodeType(int nodeType) {
        this.targetNodeType = nodeType;
    }


    public static M2CStartFaultVote create(int nodeId) {
        M2CStartFaultVote msg = new M2CStartFaultVote();
        msg.targetNodeId = nodeId;
        return msg;
    }
}
