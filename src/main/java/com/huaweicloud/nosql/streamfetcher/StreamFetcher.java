package com.huaweicloud.nosql.streamfetcher;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.google.common.util.concurrent.Uninterruptibles;
import com.huaweicloud.nosql.streamfetcher.req.DataItem;
import com.huaweicloud.nosql.streamfetcher.req.RowInfo;
import com.huaweicloud.nosql.streamfetcher.req.StreamInfo;
import com.huaweicloud.nosql.streamfetcher.req.TableEvent;
import com.huaweicloud.nosql.streamfetcher.utils.Config;
import com.huaweicloud.nosql.streamfetcher.utils.UUIDGen;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class StreamFetcher
{
    private static final long BLOCK_TIMEOUT_NS = 20000000000L;
    private static final int READ_RETRY_INTERVAL_MS = 2000;
    private static final int READ_TIMEOUT_MILLIS = 20000;

    public static List<String> GetShardIterator(Session session, String keySpace, String tableName) {
        if (session == null || keySpace == null || tableName == null) {
            System.out.println("request args are illegal \n");
            return null;
        }
        List<String> streamShardList = new ArrayList<String>();

        String querySql =
            "select stream_enabled, stream_shards, keyspace_name, table_name from system_schema.tables where keyspace_name = '"
                + keySpace + "' and table_name = '" + tableName + "'";
        ResultSet rs = session.execute(querySql);
        if (rs == null) {
            return streamShardList;
        }

        while (rs.iterator().hasNext()) {
            Row one = rs.one();
            streamShardList = (List<String>)one.getObject(1);
            break;
        }

        return streamShardList;
    }

    public static StreamInfo GetRecords(Session session, String keySpace, TableEvent tableEvent) throws Throwable {

        if (session == null || keySpace == null || tableEvent == null) {
            throw new RuntimeException("request args are illegal ");
        }

        String table = tableEvent.getTable();
        String startEventID = tableEvent.getEventID();
        String shardID = tableEvent.getShardID();
        int limitRow = tableEvent.getLimitRow();
        List<ColumnMetadata> primaryKey = tableEvent.getPrimaryKey();

        if (table == null || shardID == null || primaryKey == null) {
            throw new RuntimeException("request tableEvent args are illegal");
        }

        limitRow = limitRow > 1000 ? 1000: limitRow;

        String key = "\"" + keySpace + "\"" + "." + "\"" + table + "$streaming\"";

        String querySql;
        if (startEventID != null) {
            long timePoint = System.currentTimeMillis() - Config.GRACE_TIME;
            UUID timeUUID = UUIDGen.getTimeUUID_V2(timePoint);
            querySql = "SELECT * FROM  " + key + " where \"@eventID\" > " + startEventID + " and \"@eventID\" < " + timeUUID.toString() + " and \"@shardID\" = '"
                + shardID + "' limit "+ limitRow;
        }
        else {
            querySql = "SELECT * FROM  " + key + " where \"@shardID\" = '" + shardID + "' limit "+ limitRow;
        }
        long queryStartNanoTime = System.nanoTime();
        ResultSet rs = null;
        SimpleStatement simpleStatement = new SimpleStatement(querySql);
        simpleStatement.setReadTimeoutMillis(READ_TIMEOUT_MILLIS);
        while (true) {
            try {
                rs = session.execute(simpleStatement);
                break;
            } catch (Throwable e) {
                Throwable recursionException = new RuntimeException(e);
                boolean needRetry = false;
                while (recursionException.getCause() != null) {
                    if (recursionException.getCause() instanceof ReadTimeoutException) {
                        needRetry = true;
                        break;
                    }
                    recursionException = recursionException.getCause();
                }
                if (needRetry) {
                    if ((System.nanoTime() - queryStartNanoTime) < BLOCK_TIMEOUT_NS) {
                        Uninterruptibles.sleepUninterruptibly(READ_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
                    } else {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        }

        if (rs == null) {
            return null;
        }

        ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
        StreamInfo streamInfo = new StreamInfo();
        streamInfo.setTable(table);
        streamInfo.setShardID(shardID);

        RowInfo tmpRow = null;
        while (rs.iterator().hasNext()) {
            Row one = rs.one();
            RowInfo row = parseColumn(one, columnDefinitions, table, primaryKey);
            if (tmpRow == null) {
                tmpRow = row;
                continue;
            }
            if (!row.compare(tmpRow)) {
                streamInfo.addColumn(tmpRow);
                tmpRow = row;
            } else {
                if (row.getOldImage().size() == 0 && tmpRow.getOldImage().size() > 0) {
                    row.setOldImage(tmpRow.getOldImage());
                    streamInfo.addColumn(row);
                    tmpRow = null;
                }
                if (row.getNewImage().size() == 0 && tmpRow.getNewImage().size() > 0) {
                    row.setNewImage(tmpRow.getNewImage());
                    streamInfo.addColumn(row);
                    tmpRow = null;
                }
            }

        }

        if (tmpRow != null && !tmpRow.getOperateType().equals("UPDATE")) {
            streamInfo.addColumn(tmpRow);
        }
        return streamInfo;

    }

    private static RowInfo parseColumn(Row one, ColumnDefinitions columnDefinitions, String table, List<ColumnMetadata> primaryKey) {
        RowInfo row = new RowInfo();
        int columnPosition = 0;
        int isNewImage = 0;
        List<DataItem> colValues = new ArrayList<DataItem>();
        for (int i = 0; i < columnDefinitions.size(); i++) {
            String name = columnDefinitions.getName(i);
            if (name.equalsIgnoreCase("@eventID")) {
                DataItem eventID = getValue(null, i, one, columnDefinitions, i);
                row.setEventID(String.valueOf(eventID.getValue()));
                continue;
            }
            if (name.equalsIgnoreCase("@newOldImage")) {
                DataItem newOldImage = getValue(null, i, one, columnDefinitions, i);
                isNewImage = (Integer)newOldImage.getValue();
                continue;
            }
            if (name.equalsIgnoreCase("@eventName")) {
                DataItem eventName = getValue(null, i, one, columnDefinitions, i);
                row.setOperateType(String.valueOf(eventName.getValue()));
                continue;
            }
            if (name.equalsIgnoreCase("@shardID")) {
                continue;
            }
            for (ColumnMetadata coMetadata: primaryKey) {
                if (name.equals(coMetadata.getName())) {
                    DataItem value = getValue(name, i, one, columnDefinitions, i);
                    row.getKeys().add(value);
                }
            }
            DataItem value = getValue(name, i, one, columnDefinitions, columnPosition);
            columnPosition++;
            colValues.add(value);

        }

        if (isNewImage == 0)
        {
            row.setOldImage(colValues);
        } else {
            row.setNewImage(colValues);
        }

        return row;
    }

    private static DataItem getValue(String columnName, int index, Row one, ColumnDefinitions columnDefinitions,
        int columnPosition) {

        DataType type = columnDefinitions.getType(index);

        if (DataType.timeuuid().equals(type)) {
            return new DataItem(columnName, one.getUUID(index), type.getName().toString());
        }
        else if (DataType.ascii().equals(type)) {
            return new DataItem(columnName, one.getString(index), type.getName().toString());
        }
        else if (DataType.bigint().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());//integer
        }
        else if (DataType.cint().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());
        }
        else if (DataType.varchar().equals(type)) {
            return new DataItem(columnName, one.getString(index), type.getName().toString());
        }
        else if (DataType.cboolean().equals(type)) {
            // use getObject instead of getBoolean to fix null migration bug
            return new DataItem(columnName, one.getObject(index), type.getName().toString());
        }
        else if (DataType.blob().equals(type)) {
            return new DataItem(columnName, one.getBytes(index), type.getName().toString());//blob
        }
        else if (DataType.date().equals(type)) {
            return new DataItem(columnName, one.getDate(index), type.getName().toString());
        }
        else if (DataType.decimal().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());
        }
        else if (DataType.cdouble().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());
        }
        else if (DataType.duration().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());//********
        }
        else if (DataType.cfloat().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());
        }
        else if (DataType.inet().equals(type)) {
            return new DataItem(columnName, one.getInet(index), type.getName().toString());
        }
        else if (DataType.smallint().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());//*********
        }
        else if (DataType.text().equals(type)) {
            return new DataItem(columnName, one.getString(index), type.getName().toString());
        }
        else if (DataType.time().equals(type)) {
            return new DataItem(columnName, one.getTime(index), type.getName().toString());
        }
        else if (DataType.timestamp().equals(type)) {
            return new DataItem(columnName, one.getTimestamp(index), type.getName().toString());
        }
        else if (DataType.tinyint().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());//***************
        }
        else if (DataType.uuid().equals(type)) {
            return new DataItem(columnName, one.getUUID(index), type.getName().toString());
        }
        else if (DataType.varint().equals(type)) {
            return new DataItem(columnName, one.getObject(index), type.getName().toString());
        }

        return new DataItem(columnName, one.getObject(index), type.getName().toString());//DataType.map()//DataType.set()//DataType.list()

    }

}
