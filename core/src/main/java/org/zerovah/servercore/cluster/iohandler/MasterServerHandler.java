package org.zerovah.servercore.cluster.iohandler;

import akka.actor.ActorRef;
import org.zerovah.servercore.cluster.message.HeartbeartMsg;
import org.zerovah.servercore.cluster.message.MasterMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Master服务器IO处理类
 *
 * @author huachp
 */
public class MasterServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LogManager.getLogger(MasterServerHandler.class);

    public static final String CHECK_CONNS = "check connections";

    /** 服务端消息Actor */
    private ActorRef serverActor;

    public MasterServerHandler(ActorRef serverActor) {
        this.serverActor = serverActor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        LOGGER.info("Master客户端连接:{}", channel.remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        serverActor.tell(CHECK_CONNS, ActorRef.noSender()); // 检测连接
        LOGGER.info("服务端检测到Master客户端连接断开:{}", channel.remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            return;
        } else {
            LOGGER.error("Master消息通信过程出现异常", cause);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HeartbeartMsg) {
            HeartbeartMsg heartbeartMsg = (HeartbeartMsg) msg;
            ctx.channel().writeAndFlush(heartbeartMsg.response());
        } else if (msg instanceof MasterMessage) {
            MasterMessage masterMsg = (MasterMessage) msg;
            masterMsg.channel(ctx.channel());
            serverActor.tell(msg, ActorRef.noSender());
        } else {
            serverActor.tell(msg, ActorRef.noSender());
        }
    }

}
