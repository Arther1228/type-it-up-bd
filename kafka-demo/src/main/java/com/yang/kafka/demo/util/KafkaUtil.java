package com.yang.kafka.demo.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author yangliangchuang 2023/4/25 16:11
 */
public class KafkaUtil {

    private static String localClusterServer = "http://localhost:9092";
    private static String shinyClusterServer = "http://34.8.8.115:21005,http://34.8.8.109:21005,http://34.8.8.116:21005";
    private static int numPartitions = 3;
    private static short replicationFactor = 1;

    public static String getLocalClusterServer() {
        return localClusterServer;
    }

    public static String getShinyClusterServer() {
        return shinyClusterServer;
    }

    public static int getNumPartitions() {
        return numPartitions;
    }

    public static short getReplicationFactor() {
        return replicationFactor;
    }

    public static Properties initProperties(String bootstrap_servers) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrap_servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        // 消费者组ID
//        props.put("group.id", "test-group");
        // 开启自动提交offset
        props.put("enable.auto.commit", "true");
        // 自动提交offset的间隔时间
        props.put("auto.commit.interval.ms", "1000");

        //TODO 消费的时候，指定serializer
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 指定key和value的反序列化类
        //TODO 推送消息的时候，指定deserializer
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    /**
     * 指定消费者创建消费者
     *
     * @param bootstrapServers
     * @param groupId
     * @return
     */
    public static Consumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        return consumer;
    }

    /**
     * 创建不带groupId的消费者
     *
     * @param bootstrapServers
     * @return
     */
    public static Consumer<String, String> createEmptyConsumer(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        return consumer;
    }
}
