package org.zerovah.servercore.cluster;

/**
 * 未来执行接口(-_-!)
 *
 * @author huachp
 */
public interface INodeFuture {

    INodeFuture addConnectedListener(int nodeType, ICallback callback);

    INodeFuture addListener(ICallback callback);
}
