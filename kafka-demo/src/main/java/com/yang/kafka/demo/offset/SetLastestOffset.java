package com.yang.kafka.demo.offset;

import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 消费最新的offset数据示例
 * https://www.cnblogs.com/cocowool/p/get_kafka_latest_offset.html
 * https://github.com/cocowool/sh-valley/blob/master/java/java-kafka/src/main/java/cn/edulinks/KafkaConsumerDemo.java
 */

public class SetLastestOffset {

    private final static String TOPIC = "motorVehicleDisposition";
    private final static String groupId = "test1";

    final static Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);

    /**
     * 获取某个Topic的所有分区以及分区最新的Offset
     */
    public static void SetLastestOffset() {

        Collection<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
        System.out.println("Get the partition info as below:");
        List<TopicPartition> tp = new ArrayList<TopicPartition>();
        partitionInfos.forEach(str -> {
            System.out.println("Partition Info:");
            System.out.println(str);

            tp.add(new TopicPartition(TOPIC, str.partition()));
            consumer.assign(tp);
            consumer.seekToEnd(tp);

            System.out.println("Partition " + str.partition() + " 's latest offset is '" + consumer.position(new TopicPartition(TOPIC, str.partition())));
        });
    }

    // 持续不断的消费数据
    public static void run() {

        consumer.subscribe(Collections.singletonList(TOPIC));
        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    break;
                } else {
                    continue;
                }
            }

            // int i = 0;
            consumerRecords.forEach(record -> {
                // i = i + 1;
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
            });

            // System.out.println("Consumer Records " + i);
            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("Kafka Consumer Exited");
    }

    @Test
    public void comsumer() {
        SetLastestOffset();
        run();
    }
} 