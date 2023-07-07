package org.zerovah.servercore.net.socket;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.SystemPropertyUtil;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

public abstract class AbstractSocketServer {
    
    protected String threadName;
    protected int workerThreadCount;
    protected String host;
    protected int port;
    protected boolean reusePort; // 是否重用端口

    public String getThreadName() {
        return threadName;
    }

    public int getWorkerThreadCount() {
        return workerThreadCount;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    protected boolean isSupportReusePort() {
        if (!isUseEpollMode()) {
            return false;
        }
        String osVersion = SystemPropertyUtil.get("os.version", "").toLowerCase(Locale.US);
        String[] versionParts = osVersion.split("\\.");
        int mainVersion = Integer.parseInt(versionParts[0]);
        int subVersion = Integer.parseInt(versionParts[1]);
        if (mainVersion >= 3 && subVersion >= 9) {
            return true;
        }
        return false;
    }

    protected String formatThreadName(String suffix) {
        if (isUseEpollMode()) {
            return threadName + "_EPOLL_" + suffix;
        } else {
            return threadName + "_NORMAL_" + suffix;
        }
    }

    protected List<String> getServerHostsIpv4() throws Exception {
        LinkedList<String> ipList = new LinkedList<>();
        Enumeration<NetworkInterface> netInterfaces = null;
        netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> ips = ni.getInetAddresses();
            while (ips.hasMoreElements()) {
                InetAddress address = ips.nextElement();
                if (address instanceof Inet4Address) {
                    ipList.add(address.getHostAddress());
                }
            }
        }
        return ipList;
    }

    protected boolean isUseEpollMode() {
        return Epoll.isAvailable();
//        return !PlatformDependent.isWindows() && !PlatformDependent.isOsx();
    }

    protected EventLoopGroup newEventLoopGroup(int threadCount, String threadName) {
        DefaultThreadFactory threadFactory = new DefaultThreadFactory(threadName);
        if (isUseEpollMode()) {
            return new EpollEventLoopGroup(threadCount, threadFactory);
        } else {
            return new NioEventLoopGroup(threadCount, threadFactory);
        }
    }


    public abstract AbstractBootstrap init(ChannelInitializer<Channel> initializer);

    public abstract AbstractBootstrap bootstrap() throws Exception;


}
