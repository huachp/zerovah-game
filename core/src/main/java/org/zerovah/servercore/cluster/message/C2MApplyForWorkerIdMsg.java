package org.zerovah.servercore.cluster.message;

/**
 * 申请workerId
 *
 * @author huachp
 */
public class C2MApplyForWorkerIdMsg extends AbstractMasterCallbackStub {

    private int nodeId; // 申请的节点ID
    private int initailWorkerId; // 初始传入的workerId


    public C2MApplyForWorkerIdMsg() {
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getInitailWorkerId() {
        return initailWorkerId;
    }


    public static C2MApplyForWorkerIdMsg create(int nodeId, int initialWid) {
        C2MApplyForWorkerIdMsg message = new C2MApplyForWorkerIdMsg();
        message.nodeId = nodeId;
        message.initailWorkerId = initialWid;
        return message;
    }

}
