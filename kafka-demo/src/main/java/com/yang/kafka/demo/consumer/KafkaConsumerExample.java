package com.yang.kafka.demo.consumer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerExample {
   public static void main(String[] args) throws Exception {
      String topicName = "clicks"; // 设置要消费的主题名称
      Properties props = new Properties();
      
      // 设置Kafka broker地址
      props.put("bootstrap.servers", "localhost:9092");
      
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
         ConsumerRecords<String, String> records = consumer.poll(100);
         for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
         }
      }
   }
}
