package com.sez.flink.connectors.dynamodb.tableSink;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.sez.flink.connectors.dynamodb.util.DynamodbTableSchema;
import org.apache.flink.table.data.RowData;


public class RowDataToItemConverter implements DynamodbItemConverter<RowData> {
    DynamodbTableSchema schema;

    public RowDataToItemConverter(DynamodbTableSchema schema) {
        this.schema = schema;
    }

    @Override
    public void open() {
    }

    @Override
    public Item convertToItem(RowData record) {
        Item item = new Item();
        item.withPrimaryKey(schema.getPrimaryKeyName(),record.getString(schema.getPrimaryKeyIndex()).toString());
        for (int i=0;i<schema.getColumnNames().length;i++) {
            if (!schema.getColumnNames()[i].equals(schema.getPrimaryKeyName())) {
                item.with(schema.getColumnNames()[i],record.getString(i).toString());
            }
        }

        return item;
    }



}