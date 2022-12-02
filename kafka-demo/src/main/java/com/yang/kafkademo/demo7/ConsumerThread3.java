package com.yang.kafkademo.demo7;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * kafka消费线程
 *
 * @author admin
 */
public class ConsumerThread3 extends Thread {
    private static final Logger log = LoggerFactory.getLogger(ConsumerThread3.class);

    private KafkaConsumer<String, String> consumer;
    private String topic;

    public ConsumerThread3(KafkaConsumer<String, String> consumer, String topic) {
        this.consumer = consumer;
        this.topic = topic;
    }

    @Override
    public void run() {
        try {
            int count = 0;
            consumer.subscribe(Collections.singleton(topic));

            HashMap<Integer, Long> partitionLatestTimestamp = new HashMap<>(8);
            HashMap<Integer, List<ConsumerRecord<String, String>>> partitionLatestRecords = new HashMap<>(8);
            List<Long> timeStampList = new ArrayList<>();
            List<ConsumerRecord<String, String>> recordList = new ArrayList<>();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    System.out.printf("消费数据 partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());

                    int partitionIndex = record.partition();
                    if (!partitionLatestTimestamp.containsKey(partitionIndex)) {
                        //4102416000000 --> 2100-01-01 00:00:00
                        if (record.timestamp() >= 4102416000000L) {
                            continue;
                        }
                        timeStampList.add(record.timestamp());
                        recordList.add(record);
                        if (timeStampList.size() == 10) {
                            Long avg = timeStampList.stream().reduce(Long::sum).get() / 10;
                            timeStampList.clear();
                            partitionLatestTimestamp.put(partitionIndex, avg);

                            List<ConsumerRecord<String, String>> temp = new ArrayList<>();
                            for (ConsumerRecord<String, String> data : recordList) {
                                temp.add(data);
                            }
                            recordList.clear();
                            partitionLatestRecords.put(record.partition(), temp);
                            break;
                        }
                    }
                }
                //提交offset
                if (count % 50 == 0) {
                    consumer.commitAsync();
                    count = 0;
                }
                Thread.sleep(1);
            }
        } catch (
                Exception e) {
            log.error("ConsumerThread 出错", e);
        }
    }


}
