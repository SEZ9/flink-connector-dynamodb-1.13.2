package com.sez.flink.connectors.dynamodb.tableSource;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.sez.flink.connectors.dynamodb.util.DynamodbTableSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.util.Iterator;

public class DynamodbSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    private final String tableName;
    private DynamodbTableSchema tableSchema;
    private DeserializationSchema<RowData> deserializer;
    private DynamoDB dynamoDB;
    private Table table;
    private final int fieldLength;
    private volatile boolean isRunning = true;

    public DynamodbSourceFunction(String tableName, DynamodbTableSchema tableSchema) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.fieldLength = tableSchema.getColumnNames().length;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        while (isRunning) {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                    .withRegion(Regions.AP_SOUTHEAST_1)
                    .build();
            dynamoDB = new DynamoDB(client);
            table = dynamoDB.getTable(tableName);
            ItemCollection<ScanOutcome> items = table.scan();
            Iterator<Item> iter = items.iterator();
            while (iter.hasNext()) {
                Item item = iter.next();
                sourceContext.collect(convertToNewRow(item));
            }
        }


    }
    public RowData convertToNewRow(Item result) {
        GenericRowData resultRow = new GenericRowData(fieldLength);
        for (int i = 0; i < fieldLength; i++) {
            resultRow.setField(i, StringData.fromString(result.get(tableSchema.getColumnNames()[i]).toString()));
        }
        return resultRow;
    }


    @Override
    public void cancel() {
        isRunning = false;
        if (dynamoDB != null){
            dynamoDB.shutdown();
        }
    }
}
