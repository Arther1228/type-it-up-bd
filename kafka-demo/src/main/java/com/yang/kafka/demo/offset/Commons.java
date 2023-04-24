package com.yang.kafka.demo.offset;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author yangliangchuang 2023/4/24 11:31
 */
public class Commons {

    public final static String BOOTSTRAP_SERVERS = "34.8.8.115:21005,34.8.8.109:21005,34.8.8.116:21005,34.8.8.111:21005,34.8.8.110:21005,34.8.8.112:21005,34.8.8.113:21005,34.8.8.114:21005,34.8.8.117:21005,34.8.8.118:21005,34.8.8.119:21005,34.8.8.120:21005,34.8.8.121:21005";

    public static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-offset-check-10");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        return consumer;
    }
}
