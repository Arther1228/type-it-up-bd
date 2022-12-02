package com.yang.sparkdemo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;


/**
 * @Author He Kun
 * @Date 2019/6/26 14:38
 */
public class KafkaConsumerConfig {

    public static Map<String, Object> getConsumerParams(String groupId, String autoOffsetReset) {

        // kafka参数配置
        Map<String, Object> kafkaParams = new HashMap<>(8);
        String brokers = "34.8.8.110:21005,34.8.8.112:21005,34.8.8.119:21005,34.8.8.120:21005";
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5242880");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return kafkaParams;
    }

}
