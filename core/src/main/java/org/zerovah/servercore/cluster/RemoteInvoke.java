package org.zerovah.servercore.cluster;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 远程调用注解
 *
 * @author huachp
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RemoteInvoke {

    /**
     * 节点类型
     *
     * @return Integer
     */
    int node();

}
