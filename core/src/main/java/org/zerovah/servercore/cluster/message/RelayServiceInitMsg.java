package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.MessageDistributor;
import org.zerovah.servercore.cluster.base.DefaultNodeFuture;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.List;

public class RelayServiceInitMsg {

    private MessageDistributor distributor;
    private List<ImmutablePair> addresses;
    private DefaultNodeFuture future;

    public RelayServiceInitMsg() {
    }

    public MessageDistributor getDistributor() {
        return distributor;
    }

    public List<ImmutablePair> getAddresses() {
        return addresses;
    }

    public DefaultNodeFuture getFuture() {
        return future;
    }

    public RelayServiceInitMsg withFuture(DefaultNodeFuture future) {
        this.future = future;
        return this;
    }


    public static RelayServiceInitMsg create(MessageDistributor distributor, List selfAddrs) {
        RelayServiceInitMsg message = new RelayServiceInitMsg();
        message.addresses = selfAddrs;
        message.distributor = distributor;
        return message;
    }

}
