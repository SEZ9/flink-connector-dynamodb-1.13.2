package com.sez.flink.connectors.dynamodb.tableSink;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.flink.annotation.Internal;

import java.io.Serializable;


@Internal
public interface DynamodbItemConverter<T> extends Serializable {

    /** Initialization method for the function. It is called once before conversion method. */
    void open();


    Item convertToItem(T record);
}
