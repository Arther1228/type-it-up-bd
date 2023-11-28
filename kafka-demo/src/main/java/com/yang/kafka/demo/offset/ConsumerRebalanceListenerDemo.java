package com.yang.kafka.demo.offset;

import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author yangliangchuang 2023/4/24 11:20
 * 来自 kk-filter
 */
public class ConsumerRebalanceListenerDemo {

    private final static String TOPIC = "motorVehicle";
    private final static String groupId = "test3";

    @Test
    public void collect() {
        String startTime = "20231124";
        KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);
        SetOffset setOffset = new SetOffset(consumer, TOPIC, startTime);
        setOffset.submitOffset();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.offset() + ":" + record.partition() + ":" + record.timestamp());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * kafka设置偏移量
     */
    public static class SetOffset {

        private KafkaConsumer<String, String> consumer;
        private String topic;
        private String startTime;

        public SetOffset(KafkaConsumer<String, String> consumer, String topic, String startTime) {
            this.consumer = consumer;
            this.topic = topic;
            this.startTime = startTime;
        }

        public void submitOffset() {
            try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                Date date = simpleDateFormat.parse(startTime + "120000");
                long fallbackMilliseconds = System.currentTimeMillis() - date.getTime();

                SaveOffsetOnRebalanced saveOffsetOnRebalanced = new SaveOffsetOnRebalanced(consumer, topic, fallbackMilliseconds);
                consumer.subscribe(Collections.singleton(topic), saveOffsetOnRebalanced);
                consumer.poll(0);
            } catch (Exception e) {
                System.out.println("kafka设置从指定时间戳开始消费 SetOffset 出错");
            }
        }
    }


    /**
     * kafka设置从指定时间戳开始消费
     */
    public static class SaveOffsetOnRebalanced implements ConsumerRebalanceListener {
        private static final Logger log = LoggerFactory.getLogger(SaveOffsetOnRebalanced.class);

        private KafkaConsumer<String, String> consumer;
        private String topic;
        private long fallbackMilliseconds;

        public SaveOffsetOnRebalanced(KafkaConsumer<String, String> consumer, String topic, long fallbackMilliseconds) {
            this.consumer = consumer;
            this.topic = topic;
            this.fallbackMilliseconds = fallbackMilliseconds;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            try {
                long startTime = System.currentTimeMillis() - fallbackMilliseconds;

                Map<TopicPartition, Long> partitionLongMap = new HashMap<>();
                for (TopicPartition topicPartition : collection) {
                    partitionLongMap.put(new TopicPartition(topic, topicPartition.partition()), startTime);
                }

                Map<TopicPartition, OffsetAndTimestamp> map = consumer.offsetsForTimes(partitionLongMap);
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
                    TopicPartition partition = entry.getKey();
                    OffsetAndTimestamp value = entry.getValue();
                    long offset = value.offset();
                    consumer.seek(partition, offset);
                }
            } catch (Exception e) {
                log.error("kafka设置从指定时间戳开始消费 SaveOffsetOnRebalanced 出错", e);
            }
        }
    }
}
