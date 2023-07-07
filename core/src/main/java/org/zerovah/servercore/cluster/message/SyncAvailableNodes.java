package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.master.NodeData;

import java.util.ArrayList;
import java.util.List;

/**
 * 广播同步节点数据消息
 *
 * @author huachp
 */
public class SyncAvailableNodes {

    private int masterId; // 发送方的masterId
    private int frequencyId;
    private List<NodeData> nodeList = new ArrayList<>();
    private transient boolean retry;

    public SyncAvailableNodes() {
    }

    public int getMasterId() {
        return masterId;
    }

    public List<NodeData> getNodeList() {
        return nodeList;
    }

    public void addNodeData(NodeData nodeData) {
        nodeList.add(nodeData);
    }

    public int getFrequencyId() {
        return frequencyId;
    }

    public SyncAvailableNodes frequency(int id) {
        this.frequencyId = id;
        return this;
    }

    public void tryAgain() {
        retry = true;
    }

    public boolean isRetry() {
        return retry;
    }


    public static SyncAvailableNodes create(int masterId) {
        SyncAvailableNodes syncNodes = new SyncAvailableNodes();
        syncNodes.masterId = masterId;
        return syncNodes;
    }

    public static ResponseSyncSuccess createResponse(int myMasterId, int frequencyId) {
        ResponseSyncSuccess message = new ResponseSyncSuccess();
        message.senderMasterId = myMasterId; // 自身的MasterId
        message.frequencyId = frequencyId;
        return message;
    }


    public static class ResponseSyncSuccess {

        private int senderMasterId; // 自身的MasterId
        private int frequencyId;

        public ResponseSyncSuccess() {
        }

        public int getSenderMasterId() {
            return senderMasterId;
        }

        public int getFrequencyId() {
            return frequencyId;
        }

    }

}
