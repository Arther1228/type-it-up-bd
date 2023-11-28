package com.yang.kafka.demo.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Arrays;

/**
 * @author yangliangchuang 2023-11-28 10:34
 */
public class ConsumerUtil {

    public static void collect(String topic, String groupId) {
        Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(2000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }
}
