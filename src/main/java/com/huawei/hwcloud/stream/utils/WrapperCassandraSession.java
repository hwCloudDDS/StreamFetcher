package com.huawei.hwcloud.stream.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

/**
 * All methods here will have retry properties
 */
public class WrapperCassandraSession extends WrapperClient {

    private Session session;

    public WrapperCassandraSession(Session sess) {
        this.session = sess;
    }

    public static WrapperCassandraSession getRetrySession(Cluster cluster) {
        return RetryProxyUtils.getCassandraSession(cluster, Config.RETRY_TIMES,
            Config.RETRY_INTERVAL);
    }

    public ResultSet execute(String query) throws Exception {
        if (null != session) {
            return session.execute(query);
        }
        throw new SessionAbsentException("session absent.");
    }

    public ResultSet execute(Statement statement) throws Exception {
        if (null != session) {
            return session.execute(statement);
        }
        throw new SessionAbsentException("session absent.");
    }

    public PreparedStatement prepare(String query) throws Exception {
        if (null != session) {
            return session.prepare(query);
        }
        throw new SessionAbsentException("session absent.");
    }

    public void close() throws Exception {
        if (null != session) {
            session.close();
        }
    }

    public boolean isClose() throws Exception {
        if (null == session || session.isClosed()) {
            return true;
        }
        return false;
    }
}
