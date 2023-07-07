package org.zerovah.servercore.cluster;

/**
 * 集群逻辑处理接口
 * <p>
 * 注意:
 *    所有的NodeHandler下需要远程调用的方法,
 *    第一个参数必须是RemoteMessage的子类, 不然找不到对应的方法调用;
 *    第二个参数必须是PeersActorContext, 不然反射调用会出错;
 * <p>
 * 例子:
 * <blockquote><pre>
 *    public class SimpleHandler implements NodeHandler {
 *
 *      public void test(RemoteMessage subClassObj, PeersActorContext peersCtx) {
 *         // do something
 *      }
 *
 *    }
 * </pre></blockquote>
 *
 * @author huachp
 */
public interface NodeHandler {

}
