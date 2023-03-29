package com.yang.kafka.demo.producer;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import com.alibaba.fastjson.JSONObject;

/**
 * @author admin
 */
public class KafkaProducerExample {
    
    private static final String TOPIC_NAME = "user-info";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  
    public static void main(String[] args) {

        // 设置 Producer 属性
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 构造一个 JSON 对象作为消息体
        JSONObject message = new JSONObject();
        message.put("name1", "John");
        message.put("age2", 30);
        message.put("address3", "New York");

        // 将 JSON 对象序列化为字符串
        String messageStr = JSONObject.toJSONString(message);

        // 构造一个 ProducerRecord 对象
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageStr);

        // 发送消息
        producer.send(record);

        // 关闭生产者实例
        producer.close();
    }
}
