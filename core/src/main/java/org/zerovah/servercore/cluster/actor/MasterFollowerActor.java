package org.zerovah.servercore.cluster.actor;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.zerovah.servercore.cluster.master.MasterFollowerClient;
import org.zerovah.servercore.cluster.message.*;
import io.netty.channel.ChannelFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;

/**
 * Master主从客户端Actor
 *
 * @author huachp
 */
public class MasterFollowerActor extends AbstractActorWithTimers {

    private static final Logger LOGGER = LogManager.getLogger(MasterFollowerActor.class);

    private static final Ticker HEARTBEAT_TICKER = new Ticker();
    private static final Ticker SYNC_TICKER = new Ticker();

    private int selfMasterId; // 自身Master标识

    private TreeMap<Integer, MasterFollowerClient> clients = null;
    private HashSet<Integer> syncSuccessIds;
    private int updateSyncFrequency = 0;

    private boolean successfulInitConnections; // 成功初始化主从服务器连接
    private boolean successfulSynchronization; // 成功从备份服务器同步数据

    /** 业务节点选举切换Master, 自己获得的投票数 */
    private LinkedHashMap<Integer, C2MSwitchoverMaster> ownVotes = new LinkedHashMap<>();
    /** 业务节点投票次数记录 */
    private LinkedList<Integer> votesNumList = new LinkedList<>();

    private final Receive receive = ReceiveBuilder.create()
                    .match(HeartbeartMsg.class, this::handleHeartbeatResp)
                    .match(WriteMessageWrapper.class, this::handleWriteMessage)
                    .match(SyncAvailableNodes.class, this::handleSynchronization)
                    .match(SyncAvailableNodes.ResponseSyncSuccess.class, this::handleSyncSuccess)
                    .match(LeaderFollowerMsg.class, this::handleLeaderFollowerBusiness)
                    .match(Ticker.class, this::handleSchedule)
                    .matchAny(this::handleOtherOperation)
                    .build();

    public MasterFollowerActor(int masterId, TreeMap<Integer, MasterFollowerClient> clients) {
        this.selfMasterId = masterId;
        this.clients = clients;
    }

    public static Props props(int masterId, TreeMap<Integer, MasterFollowerClient> clients) {
        return Props.create(MasterFollowerActor.class, masterId, clients);
    }

    @Override
    public Receive createReceive() {
        return receive;
    }

    private void handleWriteMessage(WriteMessageWrapper msgWrapper) {
        int masterId = msgWrapper.getMasterId();
        if (masterId <= 0) {
            for (MasterFollowerClient syncClient : clients.values()) {
                syncClient.writeAndFlush(msgWrapper.getMessage());
            }
        } else {
            MasterFollowerClient client = clients.get(masterId);
            if (client == null) {
                LOGGER.warn("发送masterId={}的消息失败, 原因: 没有连接信息", masterId);
            } else {
                client.writeAndFlush(msgWrapper.getMessage());
            }
        }
    }

    private void handleSynchronization(SyncAvailableNodes message) {
        try {
            if (message.isRetry()) {
                if (syncSuccessIds == null || message.getFrequencyId() != updateSyncFrequency) {
                    return;
                }
                for (MasterFollowerClient syncClient : clients.values()) {
                    syncClient.writeAndFlush(message);
                }
                getTimers().startSingleTimer(message.hashCode(), message, Duration.ofMillis(5000L));
            } else {
                updateSyncFrequency ++;
                for (MasterFollowerClient syncClient : clients.values()) {
                    syncClient.writeAndFlush(message.frequency(updateSyncFrequency));
                }
                syncSuccessIds = new HashSet<>();
                message.tryAgain();
                getTimers().startSingleTimer(message.hashCode(), message, Duration.ofMillis(5000L));
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void handleSyncSuccess(SyncAvailableNodes.ResponseSyncSuccess message) {
        try {
            if (message.getFrequencyId() == updateSyncFrequency) {
                syncSuccessIds.add(message.getSenderMasterId());
                if (syncSuccessIds.size() == clients.size()) {
                    syncSuccessIds = null;
                }
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void handleHeartbeatResp(HeartbeartMsg message) {
        int masterId = message.getHeartbeatId();
        MasterFollowerClient client = clients.get(masterId);
        if (client == null) {
            LOGGER.warn("不存在masterId={}的客户端连接信息", masterId);
        } else {
            client.heartbeatSendSuccess();
        }
    }

    private void startConnect() {
        int connectedCount = 0;
        for (MasterFollowerClient client : clients.values()) {
            if (client.isConnected()) {
                connectedCount ++;
            } else if (client.connect(self())) {
                connectedCount ++;
            }
        }
        if (connectedCount >= clients.size()) {
            successfulInitConnections = true;
            LOGGER.info("所有Master主从客户端已初始化连接");
            getTimers().startSingleTimer(SYNC_TICKER.hashCode(), SYNC_TICKER, Duration.ofMillis(3000L));
        }
    }

    private void handleSchedule(Ticker ticker) {
        try {
            if (ticker == HEARTBEAT_TICKER) {
                heartbeatProcessing();
            } else if (ticker == SYNC_TICKER) {
                requestSynchronizeProcessing();
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void heartbeatProcessing() {
        if (!successfulInitConnections) {
            startConnect();
        } else {
            for (MasterFollowerClient client : clients.values()) {
                checkHeartbeat(client);
            }
            switchoverLeaderMaster();
        }
    }

    private void requestSynchronizeProcessing() {
        if (!successfulSynchronization) {
            if (clients.isEmpty()) {
                successfulSynchronization = true; return;
            }
            MasterFollowerClient client = clients.firstEntry().getValue();
            client.writeAndFlush(RequestSyncNodesMsg.create(selfMasterId));
            getTimers().startSingleTimer(SYNC_TICKER.hashCode(), SYNC_TICKER, Duration.ofMillis(3000L));
        }
    }

    private void checkHeartbeat(MasterFollowerClient client) {
        if (client.isConnected()) {
            HeartbeartMsg heartbeartMsg = HeartbeartMsg.create().injectId(client.getMasterId());
            ChannelFuture future = client.writeAndFlush(heartbeartMsg);
            future.addListener(f -> {
                if (!f.isSuccess()) {
                    if (f.cause() != null) {
                        LOGGER.error("心跳发送失败: {}", client.getConnectionAddr(), f.cause());
                    } else {
                        LOGGER.warn("心跳发送失败: {}, 无相关错误输出", client.getConnectionAddr());
                    }
                    client.closeConnection();
                }
            });
            client.recordSendHeartbeat();
            if (client.getFailedHeartbeat() >= 3) { // 失败心跳3次
                client.closeConnection();
            }
        } else {
            client.reconnect();
        }
    }

    private void handleLeaderFollowerBusiness(LeaderFollowerMsg message) {
        MasterMessage masterMsg = message.getMasterMsg();
        if (masterMsg instanceof C2MSwitchoverMaster) {
            handleSwitchoverMaster(message);
        }
    }

    private void handleSwitchoverMaster(LeaderFollowerMsg message) {
        if (selfMasterId < clients.firstKey()) {
            return;
        }
        if (clients.firstEntry().getValue().isConnected()) { // 主服务可用
            return;
        }
        C2MSwitchoverMaster switchoverMsg = (C2MSwitchoverMaster) message.getMasterMsg();
        if (ownVotes.size() > 0 && votesNumList.isEmpty()) { // 投票结束且已选定Master
            ownVotes.put(switchoverMsg.getNodeId(), switchoverMsg);
            M2CSwitchoverMaster respMsg = M2CSwitchoverMaster.create(selfMasterId);
            switchoverMsg.getChannel().writeAndFlush(respMsg);
            return;
        }

        ownVotes.put(switchoverMsg.getNodeId(), switchoverMsg);
        votesNumList.add(ownVotes.size()); // 投票数

        int repeatedVoteNum = -1;
        int maxVoteNum = votesNumList.peekLast(); // 最大票数
        Iterator<Integer> it = votesNumList.descendingIterator();
        for ( ; it.hasNext(); ) {
            Integer number = it.next();
            if (number == maxVoteNum) {
                repeatedVoteNum ++;
            }
        }
        LOGGER.info("切换Master从节点获得节点总投票数:{}, 重复票数:{}",
                maxVoteNum, repeatedVoteNum);
        if (maxVoteNum > 1 && maxVoteNum == repeatedVoteNum) { // 确定投票人个数, 且投票数一致
            M2CSwitchoverMaster respMsg = M2CSwitchoverMaster.create(selfMasterId);
            for (C2MSwitchoverMaster elementMsg : ownVotes.values()) {
                elementMsg.getChannel().writeAndFlush(respMsg);
            }
            votesNumList.clear(); // 清空投票
        }

    }

    private void switchoverLeaderMaster() {
        if (ownVotes.size() > 0) {
            Entry<Integer, MasterFollowerClient> clientEntry = clients.firstEntry();
            if (!clientEntry.getValue().isConnected()) {
                return;
            }
            LOGGER.info("主Master连接可用, 通知到所有业务节点切换回主Master");
            int leaderMasterId = clientEntry.getKey();
            M2CSwitchoverMaster broadcastMsg = M2CSwitchoverMaster.create(leaderMasterId);
            broadcastMsg.ofLeaderOrNot(true);
            Iterator<C2MSwitchoverMaster> it = ownVotes.values().iterator();
            for ( ; it.hasNext(); ) {
                C2MSwitchoverMaster switchoverMsg = it.next();
                switchoverMsg.getChannel().writeAndFlush(broadcastMsg);
            }
            ownVotes.clear(); // leader服务重新可用, 移交权限
        }
    }

    private void handleOtherOperation(Object message) {
        if (message == MServerActor.SYNCHRONIZATION_SUCCESS) {
            this.successfulSynchronization = true;
        } else if (message == MServerActor.ASK_INIT_SYNCHRONIZATION) {
            if (clients.isEmpty()) {
                sender().tell(RequestSyncNodesMsg.createResponse(), self());
            }
        } else {
            LOGGER.warn("不处理的消息: {}", message);
        }
    }

    @Override
    public void preStart() throws Exception {
        LOGGER.info("MASTER FOLLOWER ACTOR初始化启动-> {}", getSelf());
        int timerKey = HEARTBEAT_TICKER.hashCode();
        getTimers().startPeriodicTimer(timerKey, HEARTBEAT_TICKER, Duration.ofMillis(6000L));
        startConnect();
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        LOGGER.error("MASTER FOLLOWER ACTOR故障重启, ActorRef={}, Reason:{}", getSelf(), reason);
    }

    @Override
    public void postStop() throws Exception {
        LOGGER.info("MASTER FOLLOWER ACTOR停止运行, ActorRef={}", getSelf());
    }


    // 定时消息
    private static class Ticker {

    }

}
