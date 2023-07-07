package org.zerovah.servercore.cluster.master;

import io.netty.channel.Channel;

/**
 * 已注册的节点
 *
 * @author huachp
 */
public class RegisteredNode {

    private static final int INVALID_TIME = 180; // 秒

    /** 节点数据 */
    private NodeData nodeData;
    /** 持有的连接 */
    private transient Channel channel;
    /** 连接断开时间戳 */
    private int disconnectTime;
    /** 是否广播更新节点IP组配置 */
    private transient boolean updateIpGroup;

    public RegisteredNode() {
    }

    public NodeData getNodeData() {
        return nodeData;
    }

    public Channel getChannel() {
        return channel;
    }

    public void channel(Channel channel) {
        this.channel = channel;
        this.disconnectTime = 0;
    }

    public boolean isRegistered() {
        return channel != null;
    }

    public boolean isConnected() {
        boolean connected = channel != null && channel.isActive();
        if (connected) {
            if (disconnectTime > 0)
                disconnectTime = 0;
            return true;
        }
        disconnect();
        return false;
    }

    public boolean isInvalid() {
        if (disconnectTime == 0)
            return false;

        return System.currentTimeMillis() / 1000 - disconnectTime >= INVALID_TIME;
    }

    public int nodeId() {
        return nodeData.getNodeId();
    }

    public int nodeType() {
        return nodeData.getNodeType();
    }

    int disconnectTime() {
        return disconnectTime;
    }

    public void closeChannel() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    public void disconnect() {
        if (disconnectTime <= 0) {
            disconnectTime = (int) (System.currentTimeMillis() / 1000);
        }
    }

    public boolean isUpdateIpGroup() {
        return updateIpGroup;
    }

    @Override
    public String toString() {
        return nodeData.toString();
    }


    public static RegisteredNode create(NodeData nodeData) {
        RegisteredNode node = new RegisteredNode();
        node.nodeData = nodeData;
        return node;
    }

    public static RegisteredNode create(NodeData nodeData, boolean updateIpGroup) {
        RegisteredNode node = new RegisteredNode();
        node.nodeData = nodeData;
        node.updateIpGroup = updateIpGroup;
        return node;
    }
}
