package org.zerovah.servercore.cluster.message;

public class C2MLeaderFollowerServers extends AbstractMasterMsg  {

    private int nodeType = -1;
    private transient C2MInitNodeMsg initNodeMsg;

    public int getNodeType() {
        return nodeType;
    }

    public C2MInitNodeMsg takeInitNodeMsg() {
        C2MInitNodeMsg keyMsg = initNodeMsg;
        initNodeMsg = null;
        return keyMsg;
    }

    public C2MLeaderFollowerServers attachInitMsg(C2MInitNodeMsg keyMessage) {
        this.initNodeMsg = keyMessage;
        return this;
    }


    public static C2MLeaderFollowerServers create(int nodeType) {
        C2MLeaderFollowerServers message = new C2MLeaderFollowerServers();
        message.nodeType = nodeType;
        return message;
    }
}
