package org.zerovah.servercore.cluster.iohandler;

import akka.actor.ActorRef;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Master客户端IO处理类
 *
 * @author huachp
 */
public class MasterClientHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LogManager.getLogger(MasterClientHandler.class);

    /** 消息接收处理Actor */
    private ActorRef receiveActor;

    public MasterClientHandler(ActorRef receiveActor) {
        this.receiveActor = receiveActor;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("客户端检测到Master服务连接断开:{}", ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        receiveActor.tell(msg, ActorRef.noSender());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            return;
        } else {
            LOGGER.error("Master客户端收发消息异常", cause);
        }
    }
}
