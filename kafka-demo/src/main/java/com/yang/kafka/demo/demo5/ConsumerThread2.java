package com.yang.kafka.demo.demo5;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * kafka消费线程
 */
public class ConsumerThread2 extends Thread {
    private static final Logger log = LoggerFactory.getLogger(ConsumerThread2.class);

    private KafkaConsumer<String, String> consumer;
    private String topic;
    private String startTime;

    public ConsumerThread2(KafkaConsumer<String, String> consumer, String topic, String startTime) {
        this.consumer = consumer;
        this.topic = topic;
        this.startTime = startTime;
    }

    @Override
    public void run() {
        try {
            int count = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            System.out.println(record.offset() + ":" + record.partition() + ":" + record.timestamp());
                        } catch (Exception e) {
                            log.error("ConsumerThread while循环内 出错", e);
                        }
                        count++;
                    }
                    if (count % 100 == 0) {
                        consumer.commitAsync();
                    }
                }
                Thread.sleep(1);
            }
        } catch (Exception e) {
            log.error("ConsumerThread 出错", e);
        }
    }
}
