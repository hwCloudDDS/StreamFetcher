package com.huaweicloud.nosql.streamfetcher.req;

import com.datastax.driver.core.ColumnMetadata;

import java.util.List;

public class TableEvent
{

    private String table;

    private String shardID;

    private String eventID;

    private int limitRow = 100;

    private List<ColumnMetadata> primaryKey;

    public String getTable()
    {
        return table;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public String getShardID()
    {
        return shardID;
    }

    public void setShardID(String shardID)
    {
        this.shardID = shardID;
    }

    public String getEventID()
    {
        return eventID;
    }

    public void setEventID(String eventID)
    {
        this.eventID = eventID;
    }

    public int getLimitRow()
    {
        return limitRow;
    }

    public void setLimitRow(int limitRow)
    {
        this.limitRow = limitRow;
    }

    public List<ColumnMetadata> getPrimaryKey()
    {
        return primaryKey;
    }

    public void setPrimaryKey(List<ColumnMetadata> primaryKey)
    {
        this.primaryKey = primaryKey;
    }
}
