package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.iohandler.ClusterServerHandler;
import org.zerovah.servercore.net.codec.KryoDecoder;
import org.zerovah.servercore.net.codec.KryoEncoder;
import org.zerovah.servercore.net.socket.NettySocketServer;
import io.netty.bootstrap.ServerBootstrapConfig;
import io.netty.channel.*;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.flush.FlushConsolidationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.BindException;
import java.util.function.Supplier;

/**
 * 集群节点服务器
 *
 * @author huachp
 */
public class ClusterNodeServer {

    private static final Logger LOGGER = LogManager.getLogger(ClusterNodeServer.class);

    private NettySocketServer socketServer;

    private MessageDistributor distributor;

    public ClusterNodeServer() {

    }

    public NettySocketServer getSocketServer() {
        return socketServer;
    }

    public boolean start() {
        try {
            socketServer.init(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addFirst("flushBatch", new FlushConsolidationHandler());
                    // inbound
                    pipeline.addLast("lengthDecoder", new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                    pipeline.addLast("kryoDecoder", new KryoDecoder());
                    pipeline.addLast("clusterHandler", new ClusterServerHandler(distributor));
                    // outbound
                    pipeline.addLast("lengthEncoder", new LengthFieldPrepender(4));
                    pipeline.addLast("kryoEncoder", new KryoEncoder());
                }
            }).childOption(ChannelOption.SO_RCVBUF, 128 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 128 * 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true);

            socketServer.bootstrap(); // 启动
            LOGGER.info("集群节点服务启动成功, 绑定地址({}:{})", socketServer.getHost(), socketServer.getPort());
            return true;
        } catch (Exception e) {
            if (e instanceof BindException) {
                LOGGER.error("集群节点服务启动端口绑定异常, 异常消息:{}", e.getMessage());
                return false;
            }
            LOGGER.error("集群节点服务启动发生异常, 请检查逻辑 -> {}", e.getMessage());
            return false;
        }

    }

    public void startAny(Supplier<ChannelInboundHandler> handlerSupplier) {
        try {
            socketServer.init(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addFirst("flushBatch", new FlushConsolidationHandler());
                    // inbound
                    pipeline.addLast("lengthDecoder", new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                    pipeline.addLast("kryoDecoder", new KryoDecoder());
                    pipeline.addLast("anyHandler", handlerSupplier.get());
                    // outbound
                    pipeline.addLast("lengthEncoder", new LengthFieldPrepender(4));
                    pipeline.addLast("kryoEncoder", new KryoEncoder());
                }
            }).childOption(ChannelOption.SO_RCVBUF, 128 * 1024)
                    .childOption(ChannelOption.SO_SNDBUF, 128 * 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            socketServer.bootstrap(); // 启动
            LOGGER.info("Master服务启动成功, 绑定地址({}:{})", socketServer.getHost(), socketServer.getPort());
        } catch (Exception e) {
            LOGGER.error("Master服务启动发生异常, 请检查逻辑 -> {}", e.getMessage());
        }

    }

    public void stop() {
        ServerBootstrapConfig config = socketServer.getServerBootstrap().config();
        config.group().shutdownGracefully();
        config.childGroup().shutdownGracefully();
    }


    public static ClusterNodeServer create(NettySocketServer server, MessageDistributor distributor) {
        ClusterNodeServer nodeServer = new ClusterNodeServer();
        nodeServer.socketServer = server;
        nodeServer.distributor = distributor;
        return nodeServer;
    }

    public static ClusterNodeServer create(NettySocketServer server) {
        ClusterNodeServer nodeServer = new ClusterNodeServer();
        nodeServer.socketServer = server;
        return nodeServer;
    }

}
