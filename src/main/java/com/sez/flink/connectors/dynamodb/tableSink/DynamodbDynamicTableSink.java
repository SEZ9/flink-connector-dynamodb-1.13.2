package com.sez.flink.connectors.dynamodb.tableSink;

import com.sez.flink.connectors.dynamodb.util.DynamodbTableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;


public class DynamodbDynamicTableSink implements DynamicTableSink {
    private final String tableName;
    private DynamodbTableSchema tableSchema;

    public DynamodbDynamicTableSink(
            String tableName,
            DynamodbTableSchema tableSchema
    ) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            // UPSERT mode
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        DynamodbSinkFunction<RowData> sinkFunction =
                new DynamodbSinkFunction<> (
                        tableName,
                        tableSchema,
                        new RowDataToItemConverter(tableSchema)
                );
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new DynamodbDynamicTableSink(tableName,tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "Dynamodb";
    }
}
