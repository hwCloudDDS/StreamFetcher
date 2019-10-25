package com.huawei.hwcloud.stream.req;


import java.util.ArrayList;
import java.util.List;

public class StreamInfo {

    private String ShardID;

    private String Table;

    private List<RowInfo> Records = new ArrayList<>();

    public void addColumn(RowInfo row) {
        Records.add(row);
    }

    public String getShardID()
    {
        return ShardID;
    }

    public void setShardID(String shardID)
    {
        this.ShardID = shardID;
    }

    public String getTable()
    {
        return Table;
    }

    public void setTable(String table)
    {
        this.Table = table;
    }

    public List<RowInfo> getColumns()
    {
        return Records;
    }

    public void setColumns(List<RowInfo> columns)
    {
        this.Records = columns;
    }
}
