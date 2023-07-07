package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.iohandler.ClusterClientHandler;
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
 * 集群节点连接客户端
 *
 * @author huachp
 */
public class ClusterNodeClient {

    private static final Logger LOGGER = LogManager.getLogger(ClusterNodeClient.class);

    /** 对端节点的ID */
    private int nodeId;
    /** 对端节点类型 */
    private int nodeType;
    /** 对端节点的socket连接 */
    private NettySocketClient client;

    public ClusterNodeClient() {

    }

    public int getNodeId() {
        return nodeId;
    }

    public int getNodeType() {
        return nodeType;
    }

    public String getConnectedIp() {
        return client.getDefiniteHost().getHostAddress();
    }

    public NettySocketClient getClient() {
        return client;
    }

    public ChannelFuture writeAndFlush(Object message) {
        return client.writeAndFlush(message);
    }

    public void readyToConnect(ClusterContext clusterCtx) {
        readyToConnect();
        clusterCtx.registerParentNode(nodeId, this); // 注册节点连接信息
    }

    public void readyToConnect() {
        client.init(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addFirst("flushBatch", new FlushConsolidationHandler());
                // inbound
                pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));
                pipeline.addLast("kryoDecoder", new KryoDecoder());
                pipeline.addLast("handler", new ClusterClientHandler(nodeType));
                // outbound
                pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                pipeline.addLast("kryoEncoder", new KryoEncoder());
            }
        }).option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_RCVBUF, 128 * 1024)
                .option(ChannelOption.SO_SNDBUF, 128 * 1024);
    }

    public void connect() {
        try {
            client.checkHosts(); // 检查服务器地址是否可用
            client.connect();
            LOGGER.info("集群节点连接客户端启动成功, 连接地址({}:{})", client.getDefiniteHost(), client.getPort());
        } catch (Exception e) {
            LOGGER.error("集群节点连接客户端启动发生异常, 异常信息:{}", e.getMessage());
        }
    }

    public boolean reconnect() {
        try {
            client.connect();
            LOGGER.info("重连集群节点成功, 连接地址({}:{})", client.getDefiniteHost(), client.getPort());
            return true;
        } catch (Exception e) {
            LOGGER.error("重连集群节点失败, 请等待下次尝试");
            if (client.getDefiniteHost() == null) {
                client.checkHosts(); // 再次检查地址
            }
            return false;
        }
    }

    public void disconnect() {
        Channel channel = client.getChannel();
        if (channel != null) {
            channel.close();
        }
    }

    public boolean isConnect() {
        Channel channel = client.getChannel();
        return channel != null && channel.isActive();
    }

    public ClusterNodeClient ioGroup(EventLoopGroup eventLoopGroup) {
        client.initLoopGroup(eventLoopGroup);
        return this;
    }


    public static ClusterNodeClient create(int nodeId, int nodeType, NettySocketClient client) {
        ClusterNodeClient nodeClient = new ClusterNodeClient();
        nodeClient.nodeId = nodeId;
        nodeClient.nodeType = nodeType;
        nodeClient.client = client;
        return nodeClient;
    }


}
