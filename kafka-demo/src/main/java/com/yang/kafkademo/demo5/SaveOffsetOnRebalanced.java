package com.yang.kafkademo.demo5;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * kafka设置从指定时间戳开始消费
 */
public class SaveOffsetOnRebalanced implements ConsumerRebalanceListener {
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
