package com.yang.kafka.demo.offset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * @author yangliangchuang 2022/9/6 13:28
 * @link：https://www.cnblogs.com/caoweixiong/p/11684370.html
 */

public class SetFixOffsetConsumerDemo {

    private static final String TOPIC = "motorVehicle";

    private static String servers = "34.8.8.115:21005,34.8.8.109:21005,34.8.8.116:21005,34.8.8.111:21005,34.8.8.110:21005,34.8.8.112:21005,34.8.8.113:21005,34.8.8.114:21005,34.8.8.117:21005,34.8.8.118:21005,34.8.8.119:21005,34.8.8.120:21005,34.8.8.121:21005";

    private static String groupId = "kafka-offset-search-1";

    private static String autoOffsetReset = "latest";


    /**
     * 固定offset
     */
    public static void setFixOffset() {

        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
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

        for (TopicPartition tp : assignment) {
            int offset = 5200000;
            System.out.println("分区 " + tp + " 从 " + offset + " 开始消费");
            consumer.seek(tp, offset);
        }

        int count = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            // 消费记录
            for (ConsumerRecord<String, String> record : records) {
                count += records.count();
                System.out.println(record.offset() + ":" + record.partition());
            }

            if (count % 100 == 0) {
                consumer.commitAsync();
                count = 0;
            }
        }
    }

    /**
     * 从分区开头或末尾开始消费
     */
    public static void setBeginOrEndOffset() {

        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
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

        // 指定分区从头消费
//        consumer.seekToBeginning(assignment);

        // 指定分区从末尾消费
        consumer.seekToEnd(assignment);

        int count = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            // 消费记录
            for (ConsumerRecord<String, String> record : records) {
                count += records.count();
                System.out.println(record.offset() + ":" + record.value() + ":" + record.partition());
            }

            if (count % 100 == 0) {
                consumer.commitAsync();
                count = 0;
            }
        }
    }


    public static void setTimeStampOffset() {

        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
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

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        for (TopicPartition tp : assignment) {
            // 设置查询分区时间戳的条件：获取当前时间前一天之后的消息
            timestampToSearch.put(tp, System.currentTimeMillis() - 24 * 3600 * 1000);
        }

        // timestampToSearch的值为{topic-demo-0=1563709541899, topic-demo-2=1563709541899, topic-demo-1=1563709541899}
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);

        for (TopicPartition tp : assignment) {
            // 获取该分区的offset以及timestamp
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            // 如果offsetAndTimestamp不为null，则证明当前分区有符合时间戳条件的消息
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        }

        int count = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            System.out.println("##############################");
            System.out.println(records.count());

            // 消费记录
            for (ConsumerRecord<String, String> record : records) {
                count += records.count();
                System.out.println(record.offset() + ":" + record.value() + ":" + record.partition() + ":" + record.timestamp());
            }

            if (count % 100 == 0) {
                consumer.commitAsync();
                count = 0;
            }
        }
    }

    private static Properties initConfig() {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        //关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //每次拉取条数
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        return properties;
    }

    public static void main(String[] args) {
//        setFixOffset();
//        setBeginOrEndOffset();
        setTimeStampOffset();
    }
}
