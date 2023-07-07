package org.zerovah.servercore.cluster.message;

public class C2MSwitchoverMaster extends AbstractMasterMsg {

    private int nodeId;
    private int nodeType = -1;

    private boolean nodeInitialized;

    public int getNodeId() {
        return nodeId;
    }

    public int getNodeType() {
        return nodeType;
    }

    public boolean isNodeInitialized() {
        return nodeInitialized;
    }

    public void newNode(int nodeChannelId) {
        this.nodeId = nodeChannelId;
    }

    public void initializedInfo(int nodeId, int nodeType) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.nodeInitialized = true;
    }


    public static C2MSwitchoverMaster create() {
        return new C2MSwitchoverMaster();
    }

}
