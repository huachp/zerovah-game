package org.zerovah.servercore.cluster;

import java.net.InetSocketAddress;

/**
 * 集群节点信息
 *
 * @author huachp
 */
public class ClusterNode {

    private int nodeId;
    private int nodeType;
    private transient InetSocketAddress address;
    private transient boolean bindSuccess;
    private transient String[] localIps;

    public ClusterNode() {
    }

    public ClusterNode(int nodeId, int nodeType) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getNodeType() {
        return nodeType;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void bindingPort(boolean success) {
        this.bindSuccess = success;
    }

    public boolean isBindSuccess() {
        return bindSuccess;
    }

    public ClusterNode keepLocalIpList(String[] addresses) {
        this.localIps = addresses;
        return this;
    }

    public String[] getLocalIps() {
        return localIps;
    }


    public static ClusterNode create(int nodeId, int nodeType, InetSocketAddress addr) {
        ClusterNode node = new ClusterNode(nodeId, nodeType);
        node.address = addr;
        return node;
    }

}
