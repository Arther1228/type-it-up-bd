package com.yang.kafka.demo.serializer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MyMessageSerializer implements Serializer<MyMessage> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, MyMessage data) {
        if (data == null) {
            return null;
        }

        return JSON.toJSONBytes(data);
    }

    @Override
    public void close() {
        // nothing to do
    }
}
