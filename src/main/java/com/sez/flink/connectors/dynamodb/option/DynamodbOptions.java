package com.sez.flink.connectors.dynamodb.option;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@Internal
public class DynamodbOptions {
    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of Dynamodb table to connect.");
    public static final ConfigOption<String> PRIMARY_KEY =
            ConfigOptions.key("primary_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of Dynamodb table primary_key.");


}
