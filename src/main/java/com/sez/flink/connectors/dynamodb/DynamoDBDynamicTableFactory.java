package com.sez.flink.connectors.dynamodb;

import com.sez.flink.connectors.dynamodb.tableSink.DynamodbDynamicTableSink;
import com.sez.flink.connectors.dynamodb.tableSource.DynamodbDynamicTableSource;
import com.sez.flink.connectors.dynamodb.util.DynamodbTableSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.sez.flink.connectors.dynamodb.option.DynamodbOptions.PRIMARY_KEY;
import static com.sez.flink.connectors.dynamodb.option.DynamodbOptions.TABLE_NAME;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class DynamoDBDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory, TableFactory, DynamicTableFactory {

    private static final String IDENTIFIER = "dynamodb-v2";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        String primaryKeyName = tableOptions.get(PRIMARY_KEY);
        DynamodbTableSchema dynamodbTableSchema = DynamodbTableSchema.dynamodbTableSchema(tableSchema,primaryKeyName);
        String tableName = tableOptions.get(TABLE_NAME);
        return new DynamodbDynamicTableSink(tableName,dynamodbTableSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        String primaryKeyName = tableOptions.get(PRIMARY_KEY);
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        DynamodbTableSchema dynamodbTableSchema = DynamodbTableSchema.dynamodbTableSchema(tableSchema,primaryKeyName);
        String tableName = tableOptions.get(TABLE_NAME);
        final DataType producedDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return new DynamodbDynamicTableSource(tableName,dynamodbTableSchema,producedDataType);
    }



    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet();
        set.add(TABLE_NAME);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet();
        // set.add(TABLE_NAME);
        return set;
    }

    @Override
    public Map<String, String> requiredContext() {
        return null;
    }

    @Override
    public List<String> supportedProperties() {
        return null;
    }
}
