package com.yang.kafka.demo.consumer;

import java.util.Arrays;
import java.util.Properties;

import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

public class SubscribeExample {

    private final static String TOPIC = "motorVehicle";

    private final static Properties props = KafkaUtil.initProperties(KafkaUtil.getShinyClusterServer());
    // 创建KafkaConsumer实例
    private final static KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    @Test
    public void subscribe() {
        // 订阅主题
        consumer.subscribe(Arrays.asList(TOPIC));
        // 循环获取消息，直到用户强制退出
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(2000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }

}
