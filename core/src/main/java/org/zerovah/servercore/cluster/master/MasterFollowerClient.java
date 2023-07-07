package org.zerovah.servercore.cluster.master;

import akka.actor.ActorRef;
import org.zerovah.servercore.cluster.message.HeartbeartMsg;
import org.zerovah.servercore.net.codec.KryoDecoder;
import org.zerovah.servercore.net.codec.KryoEncoder;
import org.zerovah.servercore.net.socket.NettySocketClient;
import io.netty.channel.*;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.flush.FlushConsolidationHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class MasterFollowerClient {

    private static final Logger LOGGER = LogManager.getLogger(MasterFollowerClient.class);

    private int masterId;
    private NettySocketClient client;

    private int failedHeartbeat;

    public MasterFollowerClient() {
    }

    public MasterFollowerClient(int masterId) {
        this.masterId = masterId;
    }

    public int getMasterId() {
        return masterId;
    }

    public NettySocketClient getClient() {
        return client;
    }

    public ChannelFuture writeAndFlush(Object message) {
        Channel channel = client.getChannel();
        if (channel == null) {
            return null;
        }
        return channel.writeAndFlush(message);
    }

    public boolean connect(final ActorRef actorRef) {
        try {
            client.init(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();

                    pipeline.addFirst("flushBatch", new FlushConsolidationHandler());
                    // inbound
                    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                    pipeline.addLast("kryoDecoder", new KryoDecoder());
                    pipeline.addLast("handler", new MasterFollowerHandler(actorRef));
                    // outbound
                    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                    pipeline.addLast("kryoEncoder", new KryoEncoder());
                }
            }).option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_RCVBUF, 128 * 1024)
                    .option(ChannelOption.SO_SNDBUF, 128 * 1024);

            client.connect();
            LOGGER.info("Master主从客户端连接服务成功, 对端地址({}:{})", client.getDefiniteHost(), client.getPort());
            return true;
        } catch (Exception e) {
            LOGGER.error("Master主从客户端连接发生异常, {}", e.getMessage());
            return false;
        }
    }

    public boolean reconnect() {
        try {
            client.connect();
            LOGGER.info("主从客户端重连服务成功, 连接地址({}:{})", client.getDefiniteHost(), client.getPort());
            return true;
        } catch (Exception e) {
            LOGGER.error("主从客户端重连服务失败, 请等待下次尝试");
            return false;
        }
    }

    public boolean isConnected() {
        return client.getChannel() != null && client.getChannel().isActive();
    }

    public void closeConnection() {
        client.getChannel().close();
    }

    public String getConnectionAddr() {
        return client.getDefiniteHost().toString();
    }

    public void recordSendHeartbeat() {
        this.failedHeartbeat ++;
    }

    public void heartbeatSendSuccess() {
        this.failedHeartbeat = 0;
    }

    public int getFailedHeartbeat() {
        return failedHeartbeat;
    }


    public static MasterFollowerClient create(int masterId, String address, EventLoopGroup eventLoopGroup) {
        String[] addrPairs = StringUtils.split(address, ':');
        String ip = addrPairs[0];
        int port = Integer.parseInt(addrPairs[1]);
        NettySocketClient socketClient = NettySocketClient.create(ip, port);
        socketClient.initLoopGroup(eventLoopGroup);

        MasterFollowerClient mfClient = new MasterFollowerClient(masterId);
        mfClient.client = socketClient;
        return mfClient;
    }


    static class MasterFollowerHandler extends SimpleChannelInboundHandler<Object> {

        private ActorRef masterFollowerActor;

        MasterFollowerHandler(ActorRef actorRef) {
            this.masterFollowerActor = actorRef;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOGGER.info("主从客户端检测到Master服务连接断开:{}", ctx.channel().remoteAddress());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HeartbeartMsg) {
                this.masterFollowerActor.tell(msg, ActorRef.noSender());
            } else {
                LOGGER.warn("主从客户端除了心跳消息不接收其他任何消息, 请检查发送与接收流程, {}", ctx.channel().remoteAddress());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof IOException) {
                return;
            }
            LOGGER.error("主从客户端收消息处理异常", cause);
        }

    }

}
