package org.zerovah.servercore.net.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.CompleteFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.SocketUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;

public class NettySocketClient {

    private static final Logger LOGGER = LogManager.getLogger(NettySocketClient.class);

    private InetAddress[] inetHosts; // 对端地址列表
    private InetAddress definiteHost; // 确定连接的地址
    private int port;
    private Bootstrap bootstrap; // Netty客户端启动信息
    private EventLoopGroup eventLoopGroup; // IO线程池
    private Channel channel; // Netty Socket连接

    public NettySocketClient() {
    }

    NettySocketClient(InetAddress host, int port) {
        this.inetHosts = new InetAddress[0];
        this.definiteHost = host;
        this.port = port;
    }

    NettySocketClient(InetAddress[] hosts, int port) {
        this.inetHosts = hosts;
        this.port = port;
    }

    public void initLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public InetAddress[] getInetHosts() {
        return inetHosts;
    }

    public InetAddress getDefiniteHost() {
        return definiteHost;
    }

    public int getPort() {
        return port;
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public Channel getChannel() {
        return channel;
    }

    public ChannelFuture writeAndFlush(Object message) {
        if (channel == null) {
            return new EmptyChannelFuture();
        }
        return channel.writeAndFlush(message);
    }

    public Bootstrap init(ChannelInitializer<Channel> initializer) {
        if (this.eventLoopGroup == null) {
            LOGGER.error("IO线程池未初始化成功，请检查逻辑！");
            throw new RuntimeException("eventLoopGroup is null");
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroup)
                .channel(getChannelClass())
                .handler(initializer);
        return this.bootstrap = bootstrap;
    }

    public void checkHosts() {
        for (InetAddress inetHost : inetHosts) {
            // try-resources
            try (Socket socket = new Socket()) {
                SocketAddress remoteAddr = SocketUtils.socketAddress(inetHost.getHostName(), port);
                SocketUtils.connect(socket, remoteAddr, 5000);

                this.definiteHost = inetHost;
                LOGGER.info("确定连接地址:{}", inetHost);
                break;
            } catch (IOException e) {
                LOGGER.error("地址[{}]数据不可达, 尝试检测下一个地址, 异常:{}", inetHost, e.getMessage());
            }

        }
    }

    public Bootstrap connect() throws Exception {
        if (definiteHost == null) {
            throw new IOException("对端服务地址" + Arrays.toString(inetHosts) + "检测连接失败");
        }
        try {
            Channel ch = bootstrap.connect(definiteHost, port).sync().channel();
            this.channel = ch;
            return bootstrap;
        } catch (Exception e) {
            LOGGER.error("连接TCP服务器失败, Host-> {}:{}", definiteHost, port, e);
            throw e;
        }
    }

    private Class<? extends SocketChannel> getChannelClass() {
        if (!isUseEpollMode()) {
            return NioSocketChannel.class;
        }
        return EpollSocketChannel.class;
    }

    private boolean isUseEpollMode() {
        return Epoll.isAvailable();
//        return !PlatformDependent.isWindows() && !PlatformDependent.isOsx();
    }


    public static NettySocketClient create(String ipv4, int port) {
        try {
            InetAddress host = InetAddress.getByName(ipv4);
            return new NettySocketClient(host, port);
        } catch (Exception e) {
            LOGGER.error("创建NettySocketClient对象出现异常, 不合法的ipv4地址:{}", ipv4, e);
            throw new IllegalArgumentException("地址不合法:" + ipv4);
        }
    }

    public static NettySocketClient create(InetAddress[] hosts, int port) {
        return new NettySocketClient(hosts, port);
    }

    static class EmptyChannelFuture extends CompleteFuture<Void> implements ChannelFuture {

        private Throwable cause;

        public EmptyChannelFuture() {
            super(null);
        }

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public Void getNow() {
            return null;
        }

        @Override
        public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            GenericFutureListener rawTypeListener = listener;
            try {
                rawTypeListener.operationComplete(this);
            } catch (Exception e) {
                LOGGER.error("", e);
            }
            return this;
        }

        @Override
        public ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            throw new UnsupportedOperationException("不支持添加多个监听器");
        }

        @Override
        public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            return this;
        }

        @Override
        public ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return this;
        }

        @Override
        public ChannelFuture await() throws InterruptedException {
            return this;
        }

        @Override
        public ChannelFuture awaitUninterruptibly() {
            return this;
        }

        @Override
        public ChannelFuture sync() throws InterruptedException {
            return this;
        }

        @Override
        public ChannelFuture syncUninterruptibly() {
            return this;
        }

        @Override
        public boolean isVoid() {
            return true;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public Throwable cause() {
            if (cause == null) {
                cause = new NullPointerException("channel is null");
            }
            return cause;
        }
    }

}
