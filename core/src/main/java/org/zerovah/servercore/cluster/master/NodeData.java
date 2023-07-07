package org.zerovah.servercore.cluster.master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * 节点数据
 *
 * @author huachp
 */
public class NodeData {

    /** 节点ID */
    private int nodeId;
    /** 节点类型 */
    private int nodeType;
    /** socket地址信息 */
    private String[] addresses;
    /** socket端口 */
    private int port;
    /** 绑定的workerId */
    private int bindingWorkerId;

    public NodeData() {
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getNodeType() {
        return nodeType;
    }

    public String[] getAddresses() {
        return addresses;
    }

    public int getPort() {
        return port;
    }

    public int getBindingWorkerId() {
        return bindingWorkerId;
    }

    public void newPort(int port) {
        this.port = port;
    }

    public InetAddress[] getInetAddresses() {
        try {
            InetAddress[] addrArray = new InetAddress[addresses.length];
            for (int i = 0; i < addresses.length; i++) {
                addrArray[i] = InetAddress.getByName(addresses[i]);
            }
            return addrArray;
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public boolean exactMatch(int nodeType, String[] addrs, int port) {
        return this.nodeType == nodeType
                && this.port == port
                && Arrays.equals(addresses, addrs);
    }

    // 不开放其他节点赋值
    void bindWorkerId(int workerId) {
        this.bindingWorkerId = workerId;
    }

    void unbindWorkerId() {
        this.bindingWorkerId = 0;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        return b.append('{')
                .append("id=").append(nodeId)
                .append(", type=").append(nodeType)
                .append(", addrs=").append(Arrays.toString(addresses))
                .append(", port=").append(port)
                .append('}').toString();
    }

    public static NodeData create(int nodeId, int nodeType, String[] addresses, int port) {
        NodeData nodeData = new NodeData();
        nodeData.nodeId = nodeId;
        nodeData.nodeType = nodeType;
        nodeData.addresses = addresses;
        nodeData.port = port;
        return nodeData;
    }

}
