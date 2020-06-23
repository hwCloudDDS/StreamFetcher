package com.huaweicloud.nosql.streamfetcher.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class StreamRetryPolicy implements RetryPolicy {
    private int readTimes;
    private int writeTimes;
    private int unavailableTimes;

    public StreamRetryPolicy(int readTimes, int writeTimes, int unavailableTimes) {
        this.readTimes = readTimes;
        this.writeTimes = writeTimes;
        this.unavailableTimes = unavailableTimes;
    }

    public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataReceived, int rTime) {
        if (dataReceived) {
            return RetryDecision.ignore();
        } else if (rTime < readTimes) {
            return RetryDecision.retry(cl);
        } else {
            return RetryDecision.rethrow();
        }

    }

    public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt, int requiredResponses, int receivedResponses, int wTime) {
        if (wTime < writeTimes) {
            return RetryDecision.retry(cl);
        }
        return RetryDecision.rethrow();
    }

    public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, int uTime) {
        if (uTime < unavailableTimes) {
            return RetryDecision.retry(ConsistencyLevel.ONE);
        }
        return RetryDecision.rethrow();
    }

    public RetryDecision onRequestError(Statement statement, ConsistencyLevel consistencyLevel, DriverException e, int i) {
        return null;
    }

    public void init(Cluster cluster) {

    }

    public void close() {

    }

}
