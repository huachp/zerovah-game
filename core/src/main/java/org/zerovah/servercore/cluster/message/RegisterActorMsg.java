package org.zerovah.servercore.cluster.message;

/**
 * 客户端父节点Actor注册消息
 *
 * @author huachp
 */
public class RegisterActorMsg {

    private Integer nodeId;
    private boolean connectedNode;

    public RegisterActorMsg() {
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public boolean isConnectedNode() {
        return connectedNode;
    }


    public static RegisterActorMsg create(Integer nodeId) {
        RegisterActorMsg registerMsg = new RegisterActorMsg();
        registerMsg.nodeId = nodeId;
        return registerMsg;
    }

    public static RegisterActorMsg create(Integer nodeId, boolean connected) {
        RegisterActorMsg registerMsg = new RegisterActorMsg();
        registerMsg.nodeId = nodeId;
        registerMsg.connectedNode = connected;
        return registerMsg;
    }

}
