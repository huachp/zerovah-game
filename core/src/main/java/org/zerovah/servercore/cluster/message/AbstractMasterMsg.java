package org.zerovah.servercore.cluster.message;

import io.netty.channel.Channel;

/**
 * 抽象Master消息
 *
 * @author huachp
 */
public class AbstractMasterMsg implements MasterMessage {

    /** 连接渠道 */
    private Channel channel;

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void channel(Channel ch) {
        this.channel = ch;
    }

}
