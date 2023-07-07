package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.NodeLocatedIpGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * 集群节点初始化消息（Master）<p>
 * 向Master服务器请求获取nodeId和port, 并初始化
 *
 * @author huachp
 */
public class C2MInitNodeMsg extends AbstractMasterMsg {

    private int nodeType = -1;
    private String[] addresses;

    private int existingNodeId = -1;
    private int existingPort = -1;

    // 固定使用类型, 防止kryo序列化到不支持的List
    // DB核心配置项, 从DB节点传入, 由Master服务管理
    private ArrayList<NodeLocatedIpGroup> nodeIpList;
    // WORKER ID配置, 从DB节点传入, 由Master服务管理
    private ArrayList<Integer> workerIdList;

    public C2MInitNodeMsg() {
    }

    public int getNodeType() {
        return nodeType;
    }

    public String[] getAddresses() {
        return addresses;
    }

    public int getExistingNodeId() {
        return existingNodeId;
    }

    public int getExistingPort() {
        return existingPort;
    }

    public C2MInitNodeMsg existingNode(int nodeId, int port) {
        this.existingNodeId = nodeId;
        this.existingPort = port;
        return this;
    }

    public ArrayList<NodeLocatedIpGroup> getNodeIpList() {
        if (nodeIpList == null) {
            nodeIpList = new ArrayList<>(0);
        }
        return nodeIpList;
    }

    public C2MInitNodeMsg nodeGroups(List<NodeLocatedIpGroup> nodeGroups) {
        this.nodeIpList = new ArrayList<>(nodeGroups);
        return this;
    }

    public ArrayList<Integer> getWorkerIdList() {
        return workerIdList;
    }

    public C2MInitNodeMsg workerIds(List<Integer> ids) {
        this.workerIdList = new ArrayList<>(ids);
        return this;
    }


    public static C2MInitNodeMsg create(int nodeType, String[] addresses) {
        C2MInitNodeMsg msg = new C2MInitNodeMsg();
        msg.nodeType = nodeType;
        msg.addresses = addresses;
        return msg;
    }

}
