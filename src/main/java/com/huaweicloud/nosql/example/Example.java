package com.huaweicloud.nosql.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.huaweicloud.nosql.streamfetcher.StreamFetcher;
import com.huaweicloud.nosql.streamfetcher.req.RowInfo;
import com.huaweicloud.nosql.streamfetcher.req.StreamInfo;
import com.huaweicloud.nosql.streamfetcher.req.TableEvent;
import com.huaweicloud.nosql.streamfetcher.utils.StreamRetryPolicy;

import java.util.List;

public class Example
{

    public static void main(String[] args) throws Throwable {

//        RetryPolicy rc = new StreamRetryPolicy(3, 3, 2);
//        SocketOptions socketOptions = new SocketOptions().setConnectTimeoutMillis(30000);
        Cluster cluster = Cluster.builder().addContactPoint("xxx.xxx.xxx.xxx").withLoadBalancingPolicy(new RoundRobinPolicy()).withPort(9042).build();
//        Cluster cluster = Cluster.builder().addContactPointsWithPorts(ListIPs).withLoadBalancingPolicy(new RoundRobinPolicy()).withPort(port).withCredentials(username, password).build();

        List<ColumnMetadata> pk = cluster.getMetadata().getKeyspace("ks").getTable("tb1").getPrimaryKey();
        System.out.println(pk);

        Session session = cluster.connect();
        List<String> streamShards = StreamFetcher.GetShardIterator(session, "ks", "tb1");
        System.out.println(streamShards);


        TableEvent tableEvent = new TableEvent();
        tableEvent.setEventID("43e0eeb0-ee80-11e9-9c62-49626763b3dc");
        int size = 0;
        for (String shard : streamShards) {
            tableEvent.setShardID(shard);
            tableEvent.setTable("tb1");
            tableEvent.setLimitRow(100);
            tableEvent.setPrimaryKey(pk);

            StreamInfo streamInfo = StreamFetcher.GetRecords(session, "ks", tableEvent);

            Gson gson = new GsonBuilder().create();
            String line = gson.toJson(streamInfo);
            System.out.println(line);
            size += streamInfo.getColumns().size();
            System.out.println(streamInfo.getColumns().size());
            for (RowInfo rowInfo: streamInfo.getColumns()) {
                System.out.println(rowInfo.toString());
            }
        }


        System.out.println(size);
        session.close();

        System.exit(0);
    }


}
