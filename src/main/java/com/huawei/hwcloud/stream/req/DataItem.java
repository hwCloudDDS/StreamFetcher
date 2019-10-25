package com.huawei.hwcloud.stream.req;


public class DataItem
{

    private String columnName;

    private Object value;

    private String type;

    public DataItem() {
    }

    public DataItem(String columnName, Object value, String type) {
        this.columnName = columnName;
        this.value = value;
        this.type = type;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public Object getValue()
    {
        return value;
    }

    public void setValue(Object value)
    {
        this.value = value;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }
}
