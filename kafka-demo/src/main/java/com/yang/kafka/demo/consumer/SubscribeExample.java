package com.yang.kafka.demo.consumer;

import com.yang.kafka.demo.util.ConsumerUtil;
import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Test;

import java.util.Arrays;

public class SubscribeExample {

    private final static String TOPIC = "motorVehicleDisposition";
    private final static String groupId = "test1";
    private final static Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);

    @Test
    public void subscribe() {
        consumer.subscribe(Arrays.asList(TOPIC));
        ConsumerUtil.pollRecords(consumer);
    }

}
