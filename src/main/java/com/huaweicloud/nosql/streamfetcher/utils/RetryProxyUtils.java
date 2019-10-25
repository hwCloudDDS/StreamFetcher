package com.huaweicloud.nosql.streamfetcher.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

public class RetryProxyUtils implements MethodInterceptor
{

    private int retryTimes;

    private int retryInterval;

    private WrapperClient wrapperClient;

    public RetryProxyUtils(int retryTimes, int retryInterval) {
        this.retryTimes = retryTimes;
        this.retryInterval = retryInterval;
    }

    public static WrapperCassandraSession getCassandraSession(Cluster cluster, int retryTimes, int retryInterval) {
        Session session = cluster.connect();
        RetryProxyUtils proxy = new RetryProxyUtils(retryTimes, retryInterval);
        return (WrapperCassandraSession)proxy.newInstance(session);
    }

    private WrapperClient newInstance(Object object) {
        if (object instanceof Session) {
            Session session = (Session)object;
            this.wrapperClient = new WrapperCassandraSession(session);
        }
        return wrapperClient;

    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        int count = 0;
        Throwable t = null;
        while (count < retryTimes) {
            try {
                return methodProxy.invoke(this.wrapperClient, objects);
            }
            catch (Throwable e) {
                t = new Exception(e);
                Thread.sleep(retryInterval);
                count++;
            }
        }
        this.wrapperClient.close();
        if (t == null) {
            return null;
        }
        throw t;
    }
}
