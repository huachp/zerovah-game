package org.zerovah.servercore.cluster.master;

import akka.actor.ActorRef;
import org.zerovah.servercore.cluster.ClusterContext;
import org.zerovah.servercore.cluster.ProcessingCenter;
import org.zerovah.servercore.cluster.iohandler.MasterClientHandler;
import org.zerovah.servercore.net.codec.KryoDecoder;
import org.zerovah.servercore.net.codec.KryoEncoder;
import org.zerovah.servercore.net.socket.NettySocketClient;
import io.netty.channel.*;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.flush.FlushConsolidationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Master服务连接客户端
 *
 * @author huachp
 */
public class MasterClient {

    private static final Logger LOGGER = LogManager.getLogger(MasterClient.class);

    /** Master ID */
    private int masterId;
    /** Master服务连接信息 */
    private NettySocketClient client;
    /** 消息接收Actor */
    private ActorRef receiveActor;

    public MasterClient() {

    }

    public int getMasterId() {
        return masterId;
    }

    public NettySocketClient getClient() {
        return client;
    }

    public void keepMasterIdFromServer(int masterId) {
        this.masterId = masterId;
    }

    public ChannelFuture writeAndFlush(Object message) {
        return client.writeAndFlush(message);
    }

    public void disconnect() {
        Channel channel = client.getChannel();
        if (channel != null) {
            channel.close();
        }
    }

    public ActorRef connect(ClusterContext clusterCtx, ProcessingCenter enter) {
        ActorRef actorRef = clusterCtx.createMasterActor(this, enter);
        connect(actorRef);
        return receiveActor;
    }

    public ActorRef connect(ActorRef actorRef) {
        try {
            receiveActor = actorRef;
            client.init(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();

                    pipeline.addFirst("flushBatch", new FlushConsolidationHandler());
                    // inbound
                    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                    pipeline.addLast("kryoDecoder", new KryoDecoder());
                    pipeline.addLast("handler", new MasterClientHandler(receiveActor));
                    // outbound
                    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                    pipeline.addLast("kryoEncoder", new KryoEncoder());
                }
            }).option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_RCVBUF, 128 * 1024)
                    .option(ChannelOption.SO_SNDBUF, 128 * 1024);

            client.connect();
            LOGGER.info("Master[{}]客户端启动成功, 连接地址({}:{})", masterId,
                    client.getDefiniteHost(), client.getPort());
            return receiveActor;
        } catch (Exception e) {
            LOGGER.error("Master[{}]客户端启动发生异常, {}", masterId, e.getMessage());
            return receiveActor;
        }
    }

    public boolean reconnect() {
        try {
            client.connect();
            LOGGER.info("重连Master[{}]服务成功, 连接地址({}:{})", masterId,
                    client.getDefiniteHost(), client.getPort());
            return true;
        } catch (Exception e) {
            LOGGER.error("重连Master[{}]服务失败, 请等待下次尝试", masterId);
            return false;
        }
    }

    public boolean isConnect() {
        Channel channel = client.getChannel();
        return channel != null && channel.isActive();
    }

    public MasterClient ioGroup(EventLoopGroup eventLoopGroup) {
        client.initLoopGroup(eventLoopGroup);
        return this;
    }


    public static MasterClient create(NettySocketClient client, EventLoopGroup eventLoopGroup) {
        MasterClient masterClient = new MasterClient();
        masterClient.client = client;
        masterClient.ioGroup(eventLoopGroup);
        return masterClient;
    }

    public static MasterClient create(int masterId, NettySocketClient client) {
        MasterClient masterClient = new MasterClient();
        masterClient.masterId = masterId;
        masterClient.client = client;
        return masterClient;
    }

}
