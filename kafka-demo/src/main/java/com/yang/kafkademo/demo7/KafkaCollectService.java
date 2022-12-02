package com.yang.kafkademo.demo7;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * @author admin
 * 简单的kafka消费数据demo
 */
//@Service
public class KafkaCollectService {
    private static final Logger log = LoggerFactory.getLogger(KafkaCollectService.class);

    @Value("${kafka.consumer.servers}")
    private String servers;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.topic}")
    private String topic;

//    @PostConstruct
    public void start() {
        try {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            //关闭自动提交
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            //每次拉取条数
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            ConsumerThread3 consumerThread = new ConsumerThread3(consumer, topic);
            consumerThread.start();

        } catch (Exception e) {
            log.error("出错", e);
        }
    }


}
