package org.zerovah.servercore.net.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Netty udp socket服务器
 *
 * @author huachp
 */
public class NettyUdpSocketServer extends AbstractSocketServer {

    private static final Logger LOGGER = LogManager.getLogger(NettyUdpSocketServer.class);

    private Bootstrap bootstrap;

    public NettyUdpSocketServer() {
    }

    NettyUdpSocketServer(String threadName, int workerThreadCount, int port) {
        this.threadName = threadName;
        this.workerThreadCount = workerThreadCount;
        this.port = port;
        if (this.workerThreadCount <= 0) {
            this.workerThreadCount = Runtime.getRuntime().availableProcessors() * 2;  // CPU核数 * 2
        }
    }

    @Override
    public Bootstrap init(ChannelInitializer<Channel> initializer) {
        EventLoopGroup eventLoopGroup = newEventLoopGroup(workerThreadCount, formatThreadName("UDP"));

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                .channel(getChannelClass())
                .handler(initializer);
        if (reusePort = isSupportReusePort()) { // 支持使用Epoll IO模式
            bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);  // epoll端口重用
        }
        this.bootstrap = bootstrap;
        return this.bootstrap;
    }

    @Override
    public Bootstrap bootstrap() {
        if (reusePort) {
            int reusePortThreads = Runtime.getRuntime().availableProcessors();
            for (int i = 0; i < reusePortThreads; i++) {
                bindAllIpv4();
            }
            return bootstrap;
        } else {
            bindAllIpv4();
            return bootstrap;
        }
    }

    private void bindAllIpv4() {
        for (String ip : getServerHostsIpv4()) {  // udp绑定所有的ipv4
            try {
                this.bootstrap.bind(ip, port).sync();
                LOGGER.info("绑定udp地址成功, {}:{}", ip, port);
            } catch (Exception e) {
                LOGGER.error("udp服务器绑定端口出错, Host->{}:{}", ip, port, e);
                throw new RuntimeException(e);
            }
        }
    }

    private Class<? extends Channel> getChannelClass() {
        if (!isUseEpollMode()) {
            return NioDatagramChannel.class;
        }
        return EpollDatagramChannel.class;
    }

    @Override
    protected List<String> getServerHostsIpv4() {
        try {
            return super.getServerHostsIpv4();
        } catch (Exception e) {
            LOGGER.error("获取物理服务器ipv4地址出错", e);
            return Collections.emptyList();
        }
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }


    public static NettyUdpSocketServer create(String threadName, int workerThreadCount, int port) {
        return new NettyUdpSocketServer(threadName, workerThreadCount, port);
    }


}
