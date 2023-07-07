package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.master.NodeData;

import java.util.ArrayList;
import java.util.List;

/**
 * 点对点请求远程Master服务同步节点数据消息(请求方需要确认响应) <p>
 * 只会在服务启动时请求一次 <p>
 *
 * 请求流程:
 * <blockquote><pre>
 *     Requester                                   Responder
 *
 *     MasterFollowerActor(RequestSyncNodesMsg) -> MServerActor(RequestSyncNodesMsg)
 *                                                           |
 *     MServerActor(AvailableNodes)             <- MasterFollowerActor(AvailableNodes)
 * <pre><blockquote>
 *
 * @author huachp
 */
public class RequestSyncNodesMsg {

    private int masterId; // 发送方的masterId
//    private AvailableNodes respNodes;

    public RequestSyncNodesMsg() {
    }

    public int getMasterId() {
        return masterId;
    }

//    public void responseNodes(AvailableNodes nodes) {
//        this.respNodes = nodes;
//    }
//
//    public AvailableNodes getRespNodes() {
//        return respNodes;
//    }


    public static RequestSyncNodesMsg create(int masterId) {
        RequestSyncNodesMsg message = new RequestSyncNodesMsg();
        message.masterId = masterId;
        return message;
    }

    public static AvailableNodes createResponse() {
        return new AvailableNodes();
    }


    public static class AvailableNodes {

        private List<NodeData> nodeList = new ArrayList<>();

        public AvailableNodes() {
        }

        public List<NodeData> getNodeList() {
            return nodeList;
        }

        public void addNodeData(NodeData nodeData) {
            nodeList.add(nodeData);
        }

    }

}
