package org.zerovah.servercore.cluster;

import com.esotericsoftware.reflectasm.MethodAccess;
import org.zerovah.servercore.cluster.actor.ActorDistributor;
import org.zerovah.servercore.cluster.actor.PeersActorContext;
import org.zerovah.servercore.cluster.message.RemoteMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 节点逻辑处理中心
 *
 * @author huachp
 */
public class ProcessingCenter {

    private static final Logger LOGGER = LogManager.getLogger(ProcessingCenter.class);

    /** 消息处理器映射关系 */
    private Map<Class<? extends RemoteMessage>, MsgProcessor> msgProcessors = new HashMap<>();
    /** 消息枢纽中心处理器 */
    private HubHandler hubHandler;
    /** actor分发器 */
    private ActorDistributor distributor;

    public ProcessingCenter() {
    }

    public ProcessingCenter(ActorDistributor distributor) {
        this.distributor = distributor;
    }

    /**
     * 加载节点处理器类信息
     *
     * @param handlerClasses handler类对象集合
     * @param nodeType 节点类型
     */
    public void load(Collection<Class<?>> handlerClasses, int nodeType) {
        for (Class<?> cls : handlerClasses) {
            try {
                RemoteInvoke remoteInvoke = cls.getAnnotation(RemoteInvoke.class);
                if (NodeHandler.class.isAssignableFrom(cls)
                        && remoteInvoke != null && remoteInvoke.node() == nodeType) {
                    Object handlerObj = cls.newInstance();
                    registerHandler(cls, handlerObj);
                }
            } catch (Exception e) {
                LOGGER.error("节点消息处理器加载过程出现异常", e);
            }
        }
    }

    /**
     * 注册NodeHandler处理方法
     *
     * @param cls NodeHandler Class
     * @param handlerObj NodeHandler实例
     */
    private void registerHandler(Class<?> cls, Object handlerObj) {
        if (cls != handlerObj.getClass()) {
            return;
        }
        String[] publicMethodNames = getPublicMethodNames(cls);
        MethodAccess access = MethodAccess.get(cls);

        Class[][] parameterTypes = access.getParameterTypes();
        String[] methodNames = access.getMethodNames();
        for (int i = 0; i < parameterTypes.length; i++) {
            String name = methodNames[i];
            int index = Arrays.binarySearch(publicMethodNames, name);
            if (index < 0) {
                continue;
            }
            Class paramType = parameterTypes[i][0]; // 第一个参数必须是RemoteMessage的子类类型
            if (msgProcessors.get(paramType) != null) {
                LOGGER.warn("NodeHandler[{}]出现同一个{}消息绑定多个逻辑方法, 冲突方法{}",
                        cls, paramType, name);
                continue;
            }
            if (RemoteMessage.class.isAssignableFrom(paramType)) {
                Class<? extends RemoteMessage> remoteMsgClass = paramType;
                MsgProcessor msgProcessor = new MsgProcessor(handlerObj, access, i);
                msgProcessors.put(remoteMsgClass, msgProcessor);
            }
        }
    }

    private String[] getPublicMethodNames(Class<?> cls) {
        Method[] publicMethods = cls.getMethods();
        String[] names = new String[publicMethods.length];
        for (int i = 0; i < publicMethods.length; i++) {
            Method m = publicMethods[i];
            names[i] = m.getName();
        }
        Arrays.sort(names);
        return names;
    }

    public void register(Class<?>... handlerClasses) {
        for (Class<?> cls : handlerClasses) {
            try {
                if (NodeHandler.class.isAssignableFrom(cls)) {
                    Object handlerObj = cls.newInstance();
                    registerHandler(cls, handlerObj);
                }
            } catch (Exception e) {
                LOGGER.error("节点消息处理器注册消息出现异常", e);
            }
        }
    }

    /**
     * 加载节点处理器实例
     *
     * @param handlers 实例列表
     * @param nodeType 节点类型
     */
    public void loadHandlers(Collection<NodeHandler> handlers, int nodeType) {
        for (NodeHandler nodeHandler : handlers) {
            Class<?> handlerCls = nodeHandler.getClass();
            RemoteInvoke remoteInvoke = handlerCls.getAnnotation(RemoteInvoke.class);
            if (remoteInvoke != null && remoteInvoke.node() == nodeType) {
                registerHandler(handlerCls, nodeHandler);
            }
        }
    }

    public void registerHubHandler(HubHandler hubHandler) {
        this.hubHandler = hubHandler;
        this.distributor.relayHub(hubHandler);
        LOGGER.info("注册中心枢纽处理器-> {}", hubHandler);
    }

    /**
     * 反射调用，使用ReflectAsm的方式快速调用
     *
     * @param remoteMsg
     * @param peersActor
     * @param args
     */
    public Object fastInvoke(RemoteMessage remoteMsg, PeersActorContext peersActor, Object... args) {
        Class<? extends RemoteMessage> remoteMsgCls = remoteMsg.getClass();
        MsgProcessor msgProcessor = msgProcessors.get(remoteMsg.getClass());
        if (msgProcessor != null) {
            MethodAccess access = msgProcessor.getAccess();
            Object target = msgProcessor.getTarget();
            int index = msgProcessor.getMethodIdx();

            long start = System.currentTimeMillis();
            Object returnObj;
            if (args.length == 0) {
                returnObj = access.invoke(target, index, remoteMsg, peersActor);
            } else {
                Object[] formatArgs = new Object[args.length + 2];
                formatArgs[0] = remoteMsg;
                formatArgs[1] = peersActor;
                for (int i = 2, j = 0; i < formatArgs.length; i++, j++) {
                    formatArgs[i] = args[j];
                }
                returnObj = access.invoke(target, index, formatArgs);
            }
            long used = System.currentTimeMillis() - start;
            LOGGER.debug("{}远程调用耗时:{}ms", remoteMsg, used);
            return returnObj;
        } else {
            if (hubHandler != null) {
                hubHandler.forward(remoteMsg, peersActor);
            } else {
                LOGGER.error("远程调用找不到{}的调用逻辑", remoteMsgCls);
            }
            return null;
        }
    }

    public ActorDistributor getDistributor() {
        return distributor;
    }

    public ProcessingCenter holdDistributor(ActorDistributor distributor) {
        this.distributor = distributor;
        return this;
    }


    /**
     * 消息处理器
     */
    static class MsgProcessor {

        private Object target;
        private MethodAccess access;
        private int methodIdx;

        public MsgProcessor(Object target, MethodAccess access, int index) {
            this.target = target;
            this.access = access;
            this.methodIdx = index;
        }

        public Object getTarget() {
            return target;
        }

        public int getMethodIdx() {
            return methodIdx;
        }

        public MethodAccess getAccess() {
            return access;
        }
    }

}
