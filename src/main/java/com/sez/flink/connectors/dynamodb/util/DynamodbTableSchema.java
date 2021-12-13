package com.sez.flink.connectors.dynamodb.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

@Internal
public class DynamodbTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, DataType> columnMap = new LinkedHashMap<>();

    private String charset = "UTF-8";

    private PrimaryKeyInfo primaryKeyInfo;

    public String[] getColumnNames() {
        return this.columnMap.keySet().toArray(new String[0]);
    }


    public void addColumn(String column, DataType type) {
        columnMap.put(column, type);
    }

    public DataType getPrimaryKeyDataType() {
        return primaryKeyInfo == null ? null : primaryKeyInfo.primaryKeyType;
    }

    /**
     * Returns optional value of row key name. The row key name is the field name in hbase schema
     * which can be queried in Flink SQL.
     */
    public String getPrimaryKeyName() {
        return primaryKeyInfo == null ? null :primaryKeyInfo.primaryKeyName;
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyInfo == null ? -1 : primaryKeyInfo.primaryKeyIndex;
    }

    public void setPrimaryKeyInfo(String primaryKeyName, Class<?> clazz, int index) {
        Preconditions.checkNotNull(clazz, "primary key class type");
        DataType type = TypeConversions.fromLegacyInfoToDataType(TypeExtractor.getForClass(clazz));
        setPrimaryKeyInfo(primaryKeyName, type, index);
    }

    public void setPrimaryKeyInfo(String primaryKeyName, DataType type, int index) {
        Preconditions.checkNotNull(primaryKeyName, "primary key field name");
        Preconditions.checkNotNull(type, "primary key data type");
        this.primaryKeyInfo = new PrimaryKeyInfo(primaryKeyName, type, index);
    }

    private static class PrimaryKeyInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        final String primaryKeyName;
        final DataType primaryKeyType;
        final int primaryKeyIndex;

        PrimaryKeyInfo(String primaryKeyName, DataType primaryKeyType, int primaryKeyIndex) {
            this.primaryKeyName = primaryKeyName;
            this.primaryKeyType = primaryKeyType;
            this.primaryKeyIndex = primaryKeyIndex;
        }

    }

    public static DynamodbTableSchema dynamodbTableSchema(TableSchema schema, String primaryKeyName) {
        DynamodbTableSchema dynamodbTableSchema = new DynamodbTableSchema();
        String[] fieldNames = schema.getFieldNames();
        TypeInformation[] fieldTypes = schema.getFieldTypes();
        for (int i=0;i<fieldNames.length;i++) {
            Class clazz = fieldTypes[i].getTypeClass();
            dynamodbTableSchema.addColumn(fieldNames[i], TypeConversions.fromLegacyInfoToDataType(TypeExtractor.getForClass(clazz)));
            if (fieldNames[i].equals(primaryKeyName)) {
                dynamodbTableSchema.setPrimaryKeyInfo(primaryKeyName,clazz,i);
            }
        }
        return dynamodbTableSchema;
    }

    public TableSchema convertsToTableSchema() {
        String[]  fieldNames = columnMap.keySet().toArray(new String[0]);
        DataType[] dataTypes = new DataType[fieldNames.length];
        int i =0;
        for (DataType dataType: columnMap.values()){
            dataTypes[i] = dataType;
            i++;
        }
        return TableSchema.builder().fields(fieldNames,dataTypes).build();
    }

}
