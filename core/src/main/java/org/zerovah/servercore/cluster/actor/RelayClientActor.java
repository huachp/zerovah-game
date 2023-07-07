package org.zerovah.servercore.cluster.actor;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.zerovah.servercore.cluster.ClusterNode;
import org.zerovah.servercore.cluster.ClusterNodeClient;
import org.zerovah.servercore.cluster.ClusterNodeServer;
import org.zerovah.servercore.cluster.MessageDistributor;
import org.zerovah.servercore.cluster.base.Clusters;
import org.zerovah.servercore.cluster.message.CallbackMessage;
import org.zerovah.servercore.cluster.message.HeartbeartMsg;
import org.zerovah.servercore.cluster.message.RelayServiceInitMsg;
import org.zerovah.servercore.cluster.message.RemoteMessage;
import org.zerovah.servercore.net.socket.NettySocketClient;
import org.zerovah.servercore.net.socket.NettySocketServer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

/**
 * (中继节点->中继节点)中继节点互连客户端Actor
 *
 * @author huachp
 */
public class RelayClientActor extends AbstractActorWithTimers {

    public static final int RELAY_TYPE = -1;

    public static final int RELAY_NODE_ID = Short.toUnsignedInt((short) -1);

    private static final Logger LOGGER = LogManager.getLogger(RelayClientActor.class);

    private static final FaultTicker FAULT_TICKER = new FaultTicker();

    private ClusterNodeClient client;

    private ClusterNode localRelayNode;

    private String connectingAddr;

    private FaultDetectionRecord detectionRecord = new FaultDetectionRecord(); // 故障检测记录

    private final Receive receive = ReceiveBuilder.create()
            .match(RemoteMessage.class, this::despatch)
            .match(CallbackMessage.class, this::despatch)
            .match(RelayServiceInitMsg.class, this::doInitRelayService)
            .match(FaultTicker.class, this::doFaultDetection)
            .match(HeartbeartMsg.class, this::doHeartbeatResponse)
            .matchAny(this::doOtherOperation)
            .build();

    @Override
    public Receive createReceive() {
        return receive;
    }

    public RelayClientActor() {
    }

    public static Props props() {
        return Props.create(RelayClientActor.class);
    }

    private void doInitRelayService(RelayServiceInitMsg message) {
        try {
            List<ImmutablePair> selfAddrs = message.getAddresses();
            for (ImmutablePair addrPair : selfAddrs) {
                MessageDistributor distributor = message.getDistributor();
                boolean bindingSuccess = openRelayService(addrPair, distributor);
                if (bindingSuccess) {
                    relayInterconnect(addrPair);
                    message.getFuture().setAndExecute(localRelayNode);
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private boolean openRelayService(ImmutablePair addrPair, MessageDistributor distributor) {
        if (localRelayNode != null) {
            LOGGER.warn("中继节点核心逻辑已初始化, 请勿重复提交服务启动消息");
            return false;
        }
        InetSocketAddress bindingAddr = (InetSocketAddress) addrPair.getLeft();
        String threadName = "RELAY_NODE_SERVER";
        NettySocketServer socketServer = NettySocketServer.create(threadName, bindingAddr.getPort());
        ClusterNodeServer relayServer = ClusterNodeServer.create(socketServer, distributor);
        if (!relayServer.start()) {
            return false;
        }
        localRelayNode = ClusterNode.create(RELAY_NODE_ID, RELAY_TYPE, bindingAddr);
        return true;
    }

    private void relayInterconnect(ImmutablePair addrPair) {
        InetSocketAddress connectAddr = (InetSocketAddress) addrPair.getRight();
        String connectIp = connectAddr.getHostName();
        int connectPort = connectAddr.getPort();
        String threadName = "RELAY_NODE_CLIENT";

        NettySocketClient socketClient = NettySocketClient.create(connectIp, connectPort);
        ClusterNodeClient nodeClient = ClusterNodeClient.create(RELAY_NODE_ID, RELAY_TYPE, socketClient)
                .ioGroup(Clusters.newEventLoopGroup(threadName));
        nodeClient.readyToConnect();
        nodeClient.connect();
        if (nodeClient.isConnect()) {
            nodeClient.writeAndFlush(localRelayNode);
        }
        connectingAddr = connectAddr.toString();
        client = nodeClient;
    }

    private void despatch(Object message) {
        try {
            client.writeAndFlush(message);
        } catch (Exception e) {
            LOGGER.error("中继Actor转发消息出错, {}", message, e);
        }
    }

    private void doFaultDetection(FaultTicker ticker) {
        if (client.isConnect()) {
            if (detectionRecord.isHeartbeatTimeoutsExceededLimit()) {
                client.disconnect();
                LOGGER.info("对端中继节点[{}]心跳超时次数超过限制, 尝试断开重连",
                        connectingAddr);
            } else {
                sendHeartbeat();
            }
            return;
        }
        if (client.reconnect()) {
            client.writeAndFlush(localRelayNode); // 告诉对端本地节点的信息
            detectionRecord.resetDetection();
        }
    }

    private void sendHeartbeat() {
        HeartbeartMsg heartbeat = HeartbeartMsg.create();
        FaultDetectionRecord.prepareHeartbeat(self(), heartbeat);
        client.writeAndFlush(heartbeat);
        LOGGER.trace("向中继节点[{}]发送心跳消息", connectingAddr);
    }
    
    private void doHeartbeatResponse(HeartbeartMsg message) {
        try {
            if (message.isRequest()) {
                client.writeAndFlush(message.response());
            } else if (message.isResponse()) {
                detectionRecord.resetHeartbeatTimeouts();
                LOGGER.trace("收到中继节点[{}]心跳消息返回", connectingAddr);
            } else if (message.isTimeout()) {
                detectionRecord.addHeartbeatTimeouts();
                LOGGER.info("中继节点[{}]心跳消息超时, 记录", connectingAddr);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    private void doOtherOperation(Object message) {

    }


    @Override
    public void preStart() throws Exception {
        LOGGER.info("中继客户端Actor初始化启动-> {}", getSelf());
        getTimers().startPeriodicTimer(FaultTicker.class, FAULT_TICKER, Duration.ofMillis(5000L));
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        LOGGER.error("中继客户端Actor故障重启-> {}, Reason:{}", getSelf(), reason);
    }

    @Override
    public void postStop() throws Exception {
        LOGGER.info("中继客户端Actor停止运行-> {}", getSelf());
    }


    // 故障定时检测
    private static class FaultTicker {

    }
}
