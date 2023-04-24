package com.yang.kafka.demo.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaClientSubscribeExample {

    private final static String TOPIC = "motorVehicleDisposition";
    private final static String BOOTSTRAP_SERVERS = "34.8.8.115:21005,34.8.8.109:21005,34.8.8.116:21005,34.8.8.111:21005,34.8.8.110:21005,34.8.8.112:21005,34.8.8.113:21005,34.8.8.114:21005,34.8.8.117:21005,34.8.8.118:21005,34.8.8.119:21005,34.8.8.120:21005,34.8.8.121:21005";

    public static void main(String[] args) throws Exception {

        String topicName = TOPIC; // 设置要消费的主题名称
        Properties props = new Properties();

        // 设置Kafka broker地址
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        // 消费者组ID
        props.put("group.id", "test-group");

        // 开启自动提交offset
        props.put("enable.auto.commit", "true");

        // 自动提交offset的间隔时间
        props.put("auto.commit.interval.ms", "1000");

        // 指定key和value的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建KafkaConsumer实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅主题
        consumer.subscribe(Arrays.asList(topicName));

        // 循环获取消息，直到用户强制退出
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }
}
