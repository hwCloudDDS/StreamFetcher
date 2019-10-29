package com.huaweicloud.nosql.streamfetcher;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.huaweicloud.nosql.streamfetcher.req.RowInfo;
import com.huaweicloud.nosql.streamfetcher.req.StreamInfo;
import com.huaweicloud.nosql.streamfetcher.req.TableEvent;

import java.util.List;

public class Main2
{

    public static void main(String[] args) {

        Cluster cluster = Cluster.builder().addContactPoint("XXX.XXX.XXX.XXX").withLoadBalancingPolicy(new RoundRobinPolicy()).withPort(9042).build();
//        Cluster cluster = Cluster.builder().addContactPoint(endpoint).withLoadBalancingPolicy(new RoundRobinPolicy()).withPort(port).withCredentials(username, password).build();

        List<ColumnMetadata> pk = cluster.getMetadata().getKeyspace("KS").getTable("tb1").getPrimaryKey();
        System.out.println(pk);

        Session session = cluster.connect();
        List<String> streamShards = StreamFetcher.GetShardIterator(session, "KS", "tb1");
        System.out.println(streamShards);


        TableEvent tableEvent = new TableEvent();
        tableEvent.setEventID("43e0eeb0-ee80-11e9-9c62-49626763b3dc");
        tableEvent.setShardID("-4611686018427387905");
        tableEvent.setTable("tb1");
        tableEvent.setLimitRow(6);
        tableEvent.setPrimaryKey(pk);

        StreamInfo streamInfo = StreamFetcher.GetRecords(session, "KS", tableEvent);

        Gson gson = new GsonBuilder().create();
        String line = gson.toJson(streamInfo);
        System.out.println(line);

        System.out.println(streamInfo.getColumns().size());
        for (RowInfo rowInfo: streamInfo.getColumns()) {
            System.out.println(rowInfo.toString());
        }

        session.close();

        System.exit(0);
    }


}
