package org.zerovah.servercore.cluster.iohandler;

import org.zerovah.servercore.cluster.MessageDistributor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * 集群服务器IO处理类
 *
 * @author huachp
 */
public class ClusterServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LogManager.getLogger(ClusterServerHandler.class);

    /** 对端服务器节点Id */
    private int nodeId;
    /** 消息分发器 */
    private MessageDistributor distributor;

    public ClusterServerHandler(MessageDistributor distributor) {
        this.distributor = distributor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        LOGGER.info("Cluster客户端连接:{}", channel.remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if (nodeId > 0) {
            LOGGER.info("服务端检测到节点ID[{}]连接断开:{}", nodeId, channel.remoteAddress());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            return;
        } else {
            LOGGER.error("集群消息通信过程出现异常", cause);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        int peerNodeId = distributor.checkNode(msg);
        if (peerNodeId > 0) {
            nodeId = peerNodeId; return;
        }
        if (nodeId <= 0) {
            LOGGER.warn("对端节点信息尚未初始化, 处理消息可能出错:{}", msg);
        }
        distributor.distribute(nodeId, msg);
    }

}
