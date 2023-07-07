package org.zerovah.servercore.cluster.iohandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * 集群客户端IO处理类
 *
 * @author huachp
 */
public class ClusterClientHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LogManager.getLogger(ClusterClientHandler.class);

    private int nodeType;

    public ClusterClientHandler(int nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("客户端检测到类型节点[{}]连接断开:{}", nodeType, ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOGGER.warn("集群客户端不应作为业务消息接收途径, 请检查逻辑, 消息:{}", msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            return;
        } else {
            LOGGER.error("集群客户端收发消息异常", cause);
        }
    }

}
