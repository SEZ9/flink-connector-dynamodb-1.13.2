package com.sez.flink.connectors.dynamodb.tableSource;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.sez.flink.connectors.dynamodb.util.DynamodbTableSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.FunctionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Internal
public class DynamodbRowDataLookupFunction extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamodbRowDataLookupFunction.class);
    private final String tableName;
    private Table table;
    private DynamoDB dynamoDB;
    private DynamodbTableSchema tableSchema;
    private final int fieldLength;

    public DynamodbRowDataLookupFunction(
            String tableName,
            DynamodbTableSchema tableSchema
    ){
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.fieldLength = tableSchema.getColumnNames().length;
    }

    public void eval(Object key) throws IOException {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey(tableSchema.getPrimaryKeyName(),key.toString());
        Item outcome = table.getItem(spec);
        RowData rowData = convertToNewRow(outcome);
        collect(rowData);
    }

    public RowData convertToNewRow(Item result) {
        GenericRowData resultRow = new GenericRowData(fieldLength);
        for (int i = 0; i < fieldLength; i++) {
            resultRow.setField(i, StringData.fromString(result.get(tableSchema.getColumnNames()[i]).toString()));
        }
        return resultRow;
    }


    @Override
    public void open(FunctionContext context) throws Exception {
        LOG.info("start open ...");
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.AP_SOUTHEAST_1)
                .build();
        dynamoDB = new DynamoDB(client);
        table = dynamoDB.getTable(tableName);

    }

    @Override
    public void close() throws Exception {
        dynamoDB.shutdown();
    }



}
