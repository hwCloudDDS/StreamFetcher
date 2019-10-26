package com.huaweicloud.nosql.streamfetcher.req;

import java.util.ArrayList;
import java.util.List;

public class RowInfo {

    private String EventID;

    private String OperateType;

    private List<DataItem> Keys = new ArrayList<DataItem>();

    private List<DataItem> NewImage = new ArrayList<DataItem>();

    private List<DataItem> OldImage = new ArrayList<DataItem>();

    public boolean compare(RowInfo rowInfo) {
        if (rowInfo == null)
            return false;

        if (!EventID.equals(rowInfo.EventID) || !OperateType.equals(rowInfo.OperateType)) {
            return false;
        }
        for (int i = 0; i < Keys.size(); i++) {
            if (!Keys.get(i).getValue().equals(rowInfo.getKeys().get(i).getValue()))
                return false;
        }

        return true;
    }

    public String getEventID()
    {
        return EventID;
    }

    public void setEventID(String eventID)
    {
        this.EventID = eventID;
    }

    public String getOperateType()
    {
        return OperateType;
    }

    public void setOperateType(String operateType)
    {
        this.OperateType = operateType;
    }

    public List<DataItem> getKeys()
    {
        return Keys;
    }

    public void setKeys(List<DataItem> keys)
    {
        Keys = keys;
    }

    public List<DataItem> getNewImage()
    {
        return NewImage;
    }

    public void setNewImage(List<DataItem> newImage)
    {
        NewImage = newImage;
    }

    public List<DataItem> getOldImage()
    {
        return OldImage;
    }

    public void setOldImage(List<DataItem> oldImage)
    {
        OldImage = oldImage;
    }

    public void clear() {
        Keys.clear();
        NewImage.clear();
        OldImage.clear();
    }

    public String toString() {
        String str = "eventID: " + EventID + "operateType:" + OperateType;
        for (DataItem dataItem: Keys) {
            str += "Keys: columnName: " + dataItem.getColumnName();
            str += "Keys: value: " + dataItem.getValue();
            str += "Keys: type: " + dataItem.getType();
        }

        for (DataItem dataItem: NewImage) {
            str += "NewImage columnName: " + dataItem.getColumnName();
            str += "NewImage value: " + dataItem.getValue();
            str += "NewImage type: " + dataItem.getType();
        }

        for (DataItem dataItem: OldImage) {
            str += "OldImage columnName: " + dataItem.getColumnName();
            str += "OldImage value: " + dataItem.getValue();
            str += "OldImage type: " + dataItem.getType();
        }
        return str;

    }
}
