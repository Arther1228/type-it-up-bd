package com.yang.kafka.demo.producer;

import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

/**
 * @author admin
 */
public class KafkaProducerExample {

    private static final String TOPIC_NAME = "clicks5";

    @Test
    public void produce() {
        // 设置 Producer 属性
//        Properties props = Commons.initProperties(Commons.getLocalClusterServer());
        Properties props = KafkaUtil.initProperties(KafkaUtil.getShinyClusterServer());
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
