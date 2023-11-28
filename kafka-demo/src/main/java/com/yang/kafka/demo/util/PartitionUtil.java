package com.yang.kafka.demo.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;

/**
 * @author yangliangchuang 2023/11/28 15:08
 */
public class PartitionUtil {

    private static String topic = "motorVehicle";
    private static Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), topic);

    @Test
    public void test1() {
        Set<TopicPartition> assignment = getAssignment(consumer, topic);
        List<TopicPartition> topicPartitionList = getTopicPartitionList((KafkaConsumer) consumer, topic);
        System.out.println(123);
    }


    public static Set<TopicPartition> getAssignment(Consumer<String, String> consumer, String TOPIC) {

        consumer.subscribe(Arrays.asList(TOPIC));
        Set<TopicPartition> assignment = new HashSet<>();
        // 在poll()方法内部执行分区分配逻辑，该循环确保分区已被分配。
        // 当分区消息为0时进入此循环，如果不为0，则说明已经成功分配到了分区。

        while (assignment.size() == 0) {
            consumer.poll(100);
            // assignment()方法是用来获取消费者所分配到的分区消息的
            // assignment的值为：topic-demo-3, topic-demo-0, topic-demo-2, topic-demo-1
            assignment = consumer.assignment();
        }

        System.out.println(assignment);
        return assignment;
    }

    public static List<TopicPartition> getTopicPartitionList(KafkaConsumer consumer, String topic) {

        //查询 topic partitions
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfoList) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitionList.add(topicPartition);
        }

        return topicPartitionList;
    }

}
