package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.ClusterNode;

/**
 * 节点故障选举消息
 *
 * @author huachp
 */
public class C2MInitiateFaultElection extends AbstractMasterMsg {

    private int localNodeId;
    private int faultNodeId;

    public C2MInitiateFaultElection() {
    }

    public int getLocalNodeId() {
        return localNodeId;
    }

    public int getFaultNodeId() {
        return faultNodeId;
    }


    public static C2MInitiateFaultElection create(ClusterNode localNode, int faultNodeId) {
        C2MInitiateFaultElection election = new C2MInitiateFaultElection();
        election.localNodeId = localNode.getNodeId();
        election.faultNodeId = faultNodeId;
        return election;
    }
}
