package com.yang.kafka.demo.demo1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

/**
 * kafka消费线程
 *
 * @author admin
 */
public class ConsumerThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(ConsumerThread.class);

    private KafkaConsumer<String, String> consumer;
    private String topic;

    public ConsumerThread(KafkaConsumer<String, String> consumer, String topic) {
        this.consumer = consumer;
        this.topic = topic;
    }

    @Override
    public void run() {
        try {
            int count = 0;
            consumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        count++;
                        System.out.printf("消费数据 partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    }
                    //提交offset
                    if (count % 5 == 0) {
                        consumer.commitAsync();
                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            log.error("ConsumerThread 出错", e);
        }
    }


}
