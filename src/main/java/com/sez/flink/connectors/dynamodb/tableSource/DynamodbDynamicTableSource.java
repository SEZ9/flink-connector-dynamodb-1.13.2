package com.sez.flink.connectors.dynamodb.tableSource;

import com.sez.flink.connectors.dynamodb.util.DynamodbTableSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;


public class DynamodbDynamicTableSource implements LookupTableSource, ScanTableSource, SupportsProjectionPushDown {
    private final String tableName;
    private DynamodbTableSchema tableSchema;
    private final DataType producedDataType;

    public DynamodbDynamicTableSource(
            String tableName,
            DynamodbTableSchema tableSchema,
            DataType producedDataType) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.producedDataType = producedDataType;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return TableFunctionProvider.of(new DynamodbRowDataLookupFunction(tableName,tableSchema));
    }

    @Override
    public DynamicTableSource copy() {
        return new DynamodbDynamicTableSource(tableName,tableSchema,producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Dynamodb";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        final SourceFunction<RowData> sourceFunction = new DynamodbSourceFunction(tableName,tableSchema);

        return SourceFunctionProvider.of(sourceFunction,true);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        TableSchema projectSchema =
                TableSchemaUtils.projectSchema(tableSchema.convertsToTableSchema(),projectedFields);
        this.tableSchema = DynamodbTableSchema.dynamodbTableSchema(projectSchema,tableSchema.getPrimaryKeyName());
    }
}
