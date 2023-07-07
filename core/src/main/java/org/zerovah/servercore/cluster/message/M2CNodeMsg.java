package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.NodeLocatedIpGroup;
import org.zerovah.servercore.cluster.master.NodeData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 节点加入删除消息（Master）
 *
 * @author huachp
 */
public class M2CNodeMsg {

    private static final int NODE_JOINS = 1;
    private static final int NODE_EXITS = 2;

    /** 节点数据 */
    private NodeData nodeData;
    /** 节点状态 */
    private int state;
    /** 节点IP组配置 */
    private ArrayList<NodeLocatedIpGroup> ipGroups; // 只有在DB节点加入的时候才会更新

    public M2CNodeMsg() {
    }

    public NodeData getNodeData() {
        return nodeData;
    }

    public List<NodeLocatedIpGroup> getIpGroups() {
        if (ipGroups == null) {
            return Collections.emptyList();
        }
        return ipGroups;
    }

    public void syncIpGroups(ArrayList<NodeLocatedIpGroup> ipGroups) {
        this.ipGroups = ipGroups;
    }

    public boolean isNodeJoins() {
        return state == NODE_JOINS;
    }

    public boolean isNodeExits() {
        return state == NODE_EXITS;
    }

    public M2CNodeMsg nodeJoin() {
        this.state = NODE_JOINS;
        return this;
    }

    public M2CNodeMsg nodeExits() {
        this.state = NODE_EXITS;
        return this;
    }


    public static M2CNodeMsg create(NodeData nodeData) {
        M2CNodeMsg nodeMsg = new M2CNodeMsg();
        nodeMsg.nodeData = nodeData;
        return nodeMsg;
    }

}
