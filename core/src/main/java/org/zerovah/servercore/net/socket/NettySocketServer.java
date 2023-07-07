package org.zerovah.servercore.net.socket;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.NettyRuntime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Netty tcp socket服务器
 *
 * @author huachp
 */
public class NettySocketServer extends AbstractSocketServer {

    private static final Logger LOGGER = LogManager.getLogger(NettySocketServer.class);

    private ServerBootstrap serverBootstrap;
    private boolean checkThePortIsBound = true; // 检查端口是否已绑定

    public NettySocketServer() {
    }

    public NettySocketServer(String threadName, int workerThreadCount, String host, int port) {
        this.threadName = threadName;
        this.workerThreadCount = workerThreadCount;
        this.host = host;
        this.port = port;
        if (this.workerThreadCount <= 0) {
            this.workerThreadCount = NettyRuntime.availableProcessors() * 2;  // CPU核数 * 2
        }
    }

    @Override
    public ServerBootstrap init(ChannelInitializer<Channel> initializer) {
        EventLoopGroup bossGroup = newEventLoopGroup(workerThreadCount, formatThreadName("BOSS"));
        EventLoopGroup workerGroup = newEventLoopGroup(workerThreadCount, formatThreadName("WORKER"));

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(getChannelClass())
                .option(ChannelOption.SO_REUSEADDR, true)
                .childHandler(initializer);

        if (reusePort = isSupportReusePort()) { // 支持使用Epoll IO模式
            bootstrap.option(EpollChannelOption.SO_REUSEPORT, true);  // epoll端口重用
        }
        return serverBootstrap = bootstrap;
    }

    private void checkPort() throws IOException {
        if (!checkThePortIsBound)
            return;
        // try-resources
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.bind(new InetSocketAddress(port));
        } catch (IOException e) {
            LOGGER.error("进程绑定端口{}冲突", port, e);
            throw e;
        }
    }

    @Override
    public ServerBootstrap bootstrap() throws Exception {
        try {
            if (reusePort) {
                checkPort(); // 端口检查

                int reusePortThreads = Runtime.getRuntime().availableProcessors();
                for (int i = 0; i < reusePortThreads; i++) {
                    this.serverBootstrap.bind(port).sync();
                    LOGGER.info("绑定TCP端口->{}", port);
                }
            } else {
                this.serverBootstrap.bind(port).sync();
                LOGGER.info("绑定TCP端口->{}", port);
            }
            return serverBootstrap;
        } catch (Exception e) {
            LOGGER.error("TCP服务器绑定端口出错, Host->{}:{}", host, port, e);
            throw e;
        }
    }

    private Class<? extends ServerChannel> getChannelClass() {
        if (!isUseEpollMode()) {
            return NioServerSocketChannel.class;
        }
        return EpollServerSocketChannel.class;
    }

    public ServerBootstrap getServerBootstrap() {
        return serverBootstrap;
    }

    public void allowOtherProcessBoundTheSamePort() {
        this.checkThePortIsBound = false;
    }


    public static NettySocketServer create(String threadName, int workerThreadCount, int port) {
        return new NettySocketServer(threadName, workerThreadCount, "0.0.0.0", port);
    }

    public static NettySocketServer create(String threadName, int port) {
        return new NettySocketServer(threadName, -1, "0.0.0.0", port);
    }


}
