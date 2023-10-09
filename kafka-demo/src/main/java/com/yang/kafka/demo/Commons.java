package com.yang.kafka.demo;

import java.util.Properties;

/**
 * @author yangliangchuang 2023/4/25 16:11
 */
public class Commons {

    private static String local_cluster_server =  "http://localhost:9092";
    private static String shiny_cluster_server =  "http://34.8.8.115:21005,http://34.8.8.109:21005,http://34.8.8.116:21005";
    private static int numPartitions = 3;
    private static short replicationFactor = 1;

    public static String getLocalClusterServer() {
        return local_cluster_server;
    }

    public static String getShinyClusterServer() {
        return shiny_cluster_server;
    }

    public static int getNumPartitions() {
        return numPartitions;
    }

    public static short getReplicationFactor() {
        return replicationFactor;
    }

    public static Properties initProperties(String bootstrap_servers){
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_servers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        // 消费者组ID
        props.put("group.id", "test-group");
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

}
