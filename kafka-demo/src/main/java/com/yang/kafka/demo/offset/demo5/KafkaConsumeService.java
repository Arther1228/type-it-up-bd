package com.yang.kafka.demo.offset.demo5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;

/**
 * 减去固定的时间间隔完成设置偏移量
 */
//@Service
public class KafkaConsumeService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumeService.class);

    @Value("${kafka.consumer.servers}")
    private String servers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.topic}")
    private String topic;

    @Value("${startTimePrefix}")
    private String startTimePrefix;

    //@PostConstruct
    public void start() {
        try {
            subscribe(topic, startTimePrefix);
        } catch (Exception e) {
            log.error("出错", e);
        }
    }

    //开启1个线程
    public void subscribe(String topic, String startTime) {
        for (int i = 0; i < 1; i++) {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            SetOffset setOffset = new SetOffset(consumer, topic, startTime);
            setOffset.submitOffset();

            ConsumerThread2 consumerThread = new ConsumerThread2(consumer, topic, startTime);
            consumerThread.start();
        }
    }

}
