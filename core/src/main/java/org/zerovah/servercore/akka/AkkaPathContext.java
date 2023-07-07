package org.zerovah.servercore.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.zerovah.servercore.cluster.HubHandler;
import org.zerovah.servercore.cluster.ProcessingCenter;
import org.zerovah.servercore.cluster.actor.BaseMessageActor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 集群服务akka上下文
 *
 * @author huachp
 */
final public class AkkaPathContext {

    private static final Logger LOGGER = LogManager.getLogger(AkkaPathContext.class);

    private static final String SERVER_AKKA_SYSTEM_NAME = "cluster-server";

    /** akka用户监护actor路径 */
    public static final String USER_GUARDIAN = "user";
    /** akka系统监护actor路径 */
    public static final String SYSTEM_GUARDIAN = "system";

    /** 集群Actor服务系统 */
    private ActorSystem clusterServerSys;
    /** 集群逻辑处理中心 */
    private ProcessingCenter processingCenter;

    private transient HubHandler hubHandler;
    private transient List<Class<?>> customHandlers = new ArrayList<>();

    public AkkaPathContext() {
        initSystem();
    }

    private void initSystem() {
        clusterServerSys = ActorSystem.create(SERVER_AKKA_SYSTEM_NAME);
    }

    public void holdProcessingCenter(ProcessingCenter processingCenter) {
        this.processingCenter = processingCenter;
        if (hubHandler != null) {
            processingCenter.registerHubHandler(hubHandler);
        }
        processingCenter.register(customHandlers.toArray(new Class[0]));
    }

    public void registerNodeHandler(Class<?>... classes) {
        if (this.processingCenter != null) {
            this.processingCenter.register(classes);
        } else {
            Collections.addAll(this.customHandlers, classes);
        }
    }

    public void registerHubHandler(HubHandler hubHandler) {
        if (this.processingCenter != null) {
            this.processingCenter.registerHubHandler(hubHandler);
        } else {
            this.hubHandler = hubHandler;
        }
    }

    public ActorRef clusterActor(String actorName, Class<? extends BaseMessageActor> actorCls, Object... propsArgs) {
        Object[] args = new Object[propsArgs.length + 1];
        args[0] = processingCenter;
        for (int i = 1; i < args.length; i++) {
            args[i] = propsArgs[i - 1];
        }
        Props props = Props.create(actorCls, args);
        return clusterServerSys.actorOf(props, actorName);
    }

    public ActorSystem system() {
        return clusterServerSys;
    }

    public ActorRef normalActor(String actorName, Props props) {
        return clusterServerSys.actorOf(props, actorName);
    }

}
