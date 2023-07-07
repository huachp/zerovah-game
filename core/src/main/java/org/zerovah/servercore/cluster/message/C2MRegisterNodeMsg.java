package org.zerovah.servercore.cluster.message;

/**
 * 集群节点注册消息（Master）
 *
 * @author huachp
 */
public class C2MRegisterNodeMsg extends AbstractMasterMsg {

    /** 节点ID */
    private int nodeId;
    /** 端口是否已绑定 */
    private boolean portAlreadyBound;

    public C2MRegisterNodeMsg() {
    }

    public int getNodeId() {
        return nodeId;
    }

    public boolean isPortAlreadyBound() {
        return portAlreadyBound;
    }

    public static C2MRegisterNodeMsg create(int nodeId, boolean alreadyBound) {
        C2MRegisterNodeMsg msg = new C2MRegisterNodeMsg();
        msg.nodeId = nodeId;
        msg.portAlreadyBound = alreadyBound;
        return msg;
    }
}
