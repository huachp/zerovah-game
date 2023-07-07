package org.zerovah.servercore.cluster.message;


import io.netty.channel.Channel;

/**
 * Master消息接口
 *
 * @author huachp
 */
public interface MasterMessage {

    Channel getChannel();

    void channel(Channel ch);
}
