package com.yang.spark2x.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * @Author He Kun
 * @Date 2019/6/24 16:13
 */
public class KafkaProducerConfig {

    private static Producer producer = null;

    public static Producer getProducer() {

        if (producer == null) {
            String brokerList = "34.8.8.109:21005,34.8.8.110:21005,34.8.8.111:21005,34.8.8.112:21005,34.8.8.113:21005,34.8.8.114:21005,34.8.8.115:21005,34.8.8.116:21005,34.8.8.117:21005,34.8.8.118:21005,34.8.8.119:21005,34.8.8.120:21005,34.8.8.121:21005";
            Properties props = new Properties();
            props.put("bootstrap.servers", brokerList);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, String>(props);
        }
        return producer;
    }

}
