package com.sez.flink.connectors.dynamodb.tableSink;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.sez.flink.connectors.dynamodb.util.DynamodbTableSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@Internal
public class DynamodbSinkFunction<T> extends RichSinkFunction<T>
        implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(DynamodbSinkFunction.class);
    private final String tableName;
    private Table table;
    private DynamoDB dynamoDB;
    private DynamodbTableSchema schema;
    private final int fieldLength;
    private DynamodbItemConverter dynamodbItemConverter;

    private Counter counter;

    public DynamodbSinkFunction(
            String tableName,
            DynamodbTableSchema schema,
            DynamodbItemConverter dynamodbItemConverter
    ) {
        this.tableName = tableName;
        this.schema = schema;
        this.fieldLength = schema.getColumnNames().length;
        this.dynamodbItemConverter = dynamodbItemConverter;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("start open ...");
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.AP_SOUTHEAST_1)
                .build();
        dynamoDB = new DynamoDB(client);
        table = dynamoDB.getTable(tableName);
        this.counter = getRuntimeContext().getMetricGroup().counter("DynamodbSink");
    }



    @Override
    public void close() throws Exception {
        dynamoDB.shutdown();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        table.putItem(dynamodbItemConverter.convertToItem(value));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

}
