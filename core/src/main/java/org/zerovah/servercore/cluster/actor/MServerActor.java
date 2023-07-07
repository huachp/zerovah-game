package org.zerovah.servercore.cluster.actor;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.zerovah.servercore.cluster.master.MasterContext;
import org.zerovah.servercore.cluster.master.NodeData;
import org.zerovah.servercore.cluster.master.RegisteredNode;
import org.zerovah.servercore.cluster.message.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.zerovah.servercore.cluster.iohandler.MasterServerHandler.CHECK_CONNS;

/**
 * Master服务器Actor, 服务启动后只有一个MServerActor对象
 *
 * @author huachp
 */
public class MServerActor extends AbstractActorWithTimers {

    static final String SYNCHRONIZATION_SUCCESS = "master sync init success";
    static final String ASK_INIT_SYNCHRONIZATION = "ask init sync";

    private static final Logger LOGGER = LogManager.getLogger(MServerActor.class);

    private MasterContext masterCtx;
    private boolean initFinished;

    private LinkedList<MasterMessage> msgQueue = new LinkedList<>();

    private final Receive receive = ReceiveBuilder.create()
            .match(MasterMessage.class, this::processMasterMessage)
            .match(RequestSyncNodesMsg.class, this::processReplication)
            .match(RequestSyncNodesMsg.AvailableNodes.class, this::processResponseSyncNodes)
            .match(SyncAvailableNodes.class, this::processUpdateSyncNodes)
            .match(SyncAvailableNodes.ResponseSyncSuccess.class, this::notifyMfActorSyncSuccess)
            .matchAny(this::processOtherMsg)
            .build();

    public MServerActor(MasterContext masterCtx) {
        this.masterCtx = masterCtx;
    }

    public static Props props(MasterContext masterCtx) {
        return Props.create(MServerActor.class, masterCtx);
    }

    @Override
    public Receive createReceive() {
        return receive;
    }

    private void enqueue(MasterMessage masterMsg) {
        msgQueue.addLast(masterMsg);
    }

    private void processMasterMessage(MasterMessage message) {
        try {
            if (!initFinished) {
                enqueue(message);
                if (msgQueue.size() == 1) {
                    getTimers().startSingleTimer(1, ASK_INIT_SYNCHRONIZATION, Duration.ofMillis(10000L));
                }
                return;
            }
            if (message instanceof C2MLeaderFollowerServers) {
                processGetMastersAddressInfo( (C2MLeaderFollowerServers) message );
            } else if (message instanceof C2MInitNodeMsg) {
                processInitNode( (C2MInitNodeMsg) message );
            } else if (message instanceof C2MRegisterNodeMsg) {
                processRegisterNode( (C2MRegisterNodeMsg) message );
            } else if (message instanceof C2MInitiateFaultElection) {
                processFaultElection( (C2MInitiateFaultElection) message );
            } else if (message instanceof C2MNodeFaultVote) {
                processVote( (C2MNodeFaultVote) message );
            } else if (message instanceof C2MSwitchoverMaster) {
                processSwitchoverMaster( (C2MSwitchoverMaster) message );
            } else if (message instanceof C2MApplyForWorkerIdMsg) {
                processApplyWorkerId( (C2MApplyForWorkerIdMsg) message );
            }
        } catch (Exception e) {
            LOGGER.error("处理Master消息发生异常", e);
        }
    }

    private void processReplication(RequestSyncNodesMsg message) {
        try {
            RequestSyncNodesMsg.AvailableNodes nodesResp = RequestSyncNodesMsg.createResponse();
            Collection<RegisteredNode> nodes = masterCtx.registeredNodesValues();
            for (RegisteredNode node : nodes) {
                nodesResp.addNodeData(node.getNodeData());
            }
            WriteMessageWrapper msgWrapper = WriteMessageWrapper.create(message.getMasterId(), nodesResp);
            masterCtx.getMfClientActorRef().tell(msgWrapper, self());
        } catch (Exception e) {
            LOGGER.error("处理Replication发生异常", e);
        }
    }

    private void processResponseSyncNodes(RequestSyncNodesMsg.AvailableNodes message) {
        try {
            List<NodeData> syncNodes = message.getNodeList();
            masterCtx.syncRegisteredNodes(syncNodes);
            initFinished = true;
            masterCtx.getMfClientActorRef().tell(SYNCHRONIZATION_SUCCESS, self());

            int maxNodeId = 0;
            int maxPort = 0;
            for (MasterMessage masterMsg : msgQueue) {
                if (masterMsg instanceof C2MInitNodeMsg) {
                    int existNode = ((C2MInitNodeMsg) masterMsg).getExistingNodeId();
                    int existPort = ((C2MInitNodeMsg) masterMsg).getExistingPort();
                    if (existNode > 0) {
                        maxNodeId = Math.max(maxNodeId, existNode);
                        maxPort = Math.max(maxPort, existPort);
                    }
                }
            }
            if (maxNodeId > 0) {
                masterCtx.continuedNodeAllocationInit(maxNodeId, maxPort);
            }
            int currentCount = msgQueue.size();
            for (int i = 0; i < currentCount; i++) {
                processMasterMessage(msgQueue.poll());
            }
            LOGGER.info("Master Server初始同步节点数据结束");
        } catch (Exception e) {
            LOGGER.error("节点同步响应初始化发生异常", e);
        }
    }

    private void processUpdateSyncNodes(SyncAvailableNodes message) {
        try {
            List<NodeData> syncNodes = message.getNodeList();
            masterCtx.syncRegisteredNodes(syncNodes);

            ActorRef mfActor = masterCtx.getMfClientActorRef();
            int myMasterId = masterCtx.getMasterId();
            int frequencyId = message.getFrequencyId();

            SyncAvailableNodes.ResponseSyncSuccess resp = SyncAvailableNodes.createResponse(myMasterId, frequencyId);
            WriteMessageWrapper msgWrapper = WriteMessageWrapper.create(message.getMasterId(), resp);
            mfActor.tell(msgWrapper, self());
        } catch (Exception e) {
            LOGGER.error("更新同步节点发生异常", e);
        }
    }

    private void notifyMfActorSyncSuccess(SyncAvailableNodes.ResponseSyncSuccess message) {
        masterCtx.getMfClientActorRef().tell(message, self());
    }


    private void processGetMastersAddressInfo(C2MLeaderFollowerServers serverInfoMsg) {
        masterCtx.requestMasterFollowersAddress(serverInfoMsg);
    }

    private void processInitNode(C2MInitNodeMsg nodeMsg) {
        masterCtx.initNode(nodeMsg);
    }

    private void processRegisterNode(C2MRegisterNodeMsg registerMsg) {
        masterCtx.registerNode(registerMsg);
    }

    private void processFaultElection(C2MInitiateFaultElection msg) {
        masterCtx.initiateFaultElection(msg);
    }

    private void processVote(C2MNodeFaultVote msg) {
        masterCtx.nodeFaultVote(msg);
    }

    private void processSwitchoverMaster(C2MSwitchoverMaster switchoverMsg) {
        masterCtx.switchoverMasterVote(switchoverMsg);
    }

    private void processApplyWorkerId(C2MApplyForWorkerIdMsg applyMsg) {
        masterCtx.assignWorkerIdToNode(applyMsg);
    }

    private void processOtherMsg(Object message) {
        if (message == ASK_INIT_SYNCHRONIZATION) {
            masterCtx.getMfClientActorRef().tell(message, self());
        } else if (message == CHECK_CONNS) {
            masterCtx.checkNodeConnections();
        } else {
            LOGGER.warn("收到不合法的消息: {}", message);
        }
    }

    @Override
    public void preStart() throws Exception {
        LOGGER.info("Master Server Actor初始化启动-> {}", getSelf());
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        LOGGER.error("Master Server Actor故障重启-> {}, Reason:{}", getSelf(), reason);
    }

    @Override
    public void postStop() throws Exception {
        LOGGER.info("Master Server Actor停止运行-> {}", getSelf());
    }

}
