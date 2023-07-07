package org.zerovah.servercore.cluster.message;

/**
 * 故障节点投票
 *
 * @author huachp
 */
public class C2MNodeFaultVote extends AbstractMasterMsg  {

    private int localNodeId;
    private int faultNodeId;

    public C2MNodeFaultVote() {
    }

    public int getLocalNodeId() {
        return localNodeId;
    }

    public int getFaultNodeId() {
        return faultNodeId;
    }


    public static C2MNodeFaultVote create(int localNodeId, int faultNodeId) {
        C2MNodeFaultVote vote = new C2MNodeFaultVote();
        vote.localNodeId = localNodeId;
        vote.faultNodeId = faultNodeId;
        return vote;
    }
}
