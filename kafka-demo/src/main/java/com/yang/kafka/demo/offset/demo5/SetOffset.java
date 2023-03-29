package com.yang.kafka.demo.offset.demo5;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;

/**
 * kafka设置从指定时间戳开始消费
 */
public class SetOffset {
    private static final Logger log = LoggerFactory.getLogger(SetOffset.class);

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
            consumer.poll(Duration.ofMillis(0));
        } catch (Exception e) {
            log.error("kafka设置从指定时间戳开始消费 SetOffset 出错", e);
        }
    }
}
