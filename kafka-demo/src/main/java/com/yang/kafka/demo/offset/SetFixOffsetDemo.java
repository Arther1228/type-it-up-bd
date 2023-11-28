package com.yang.kafka.demo.offset;

import com.yang.kafka.demo.util.ConsumerUtil;
import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.*;

/**
 * @author yangliangchuang 2022/9/6 13:28
 */

public class SetFixOffsetDemo {

    private static final String TOPIC = "motorVehicle";

    private static String servers = "34.8.8.115:21005,34.8.8.109:21005,34.8.8.116:21005,34.8.8.111:21005,34.8.8.110:21005,34.8.8.112:21005,34.8.8.113:21005,34.8.8.114:21005,34.8.8.117:21005,34.8.8.118:21005,34.8.8.119:21005,34.8.8.120:21005,34.8.8.121:21005";

    private static String groupId = "kafka-offset-search-1";

    private static String autoOffsetReset = "latest";


    /**
     * 固定offset
     * https://www.cnblogs.com/caoweixiong/p/11684370.html
     */
    @Test
    public void setFixOffset() {

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

        ConsumerUtil.collect(TOPIC, groupId);
    }

    /**
     * @return https://blog.csdn.net/jj89929665/article/details/121681927
     */
    @Test
    public void reduceFixOffset() {

        int target = 1;
        final Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("获取订阅-开始拉去数据");
        ConsumerRecords<String, String> records = consumer.poll(10000);
        System.err.println("偏移量记录位置为: " + records.count());
        System.err.println("希望偏移量参数位置为: " + target);

        List<PartitionInfo> list = consumer.partitionsFor(TOPIC);
        System.err.println(TOPIC + " 主题 的分区数为:" + list.size());

        List<TopicPartition> topicList = new ArrayList<TopicPartition>();
        for (PartitionInfo pt : list) {
            TopicPartition tp = new TopicPartition(TOPIC, pt.partition());
            topicList.add(tp);
        }

        Map<TopicPartition, Long> endMap = consumer.endOffsets(topicList);
        Map<TopicPartition, Long> beginmap = consumer.beginningOffsets(topicList);

        long aimOffset;
        for (TopicPartition tp : topicList) {
            System.err.println("消费者为" + tp);
            long endOffset = endMap.get(tp);
            long beginOffset = beginmap.get(tp);

            aimOffset = endOffset - target;
            System.err.println("topic数据总量为:" + (endOffset - beginOffset));
            if (aimOffset > 0 && aimOffset >= beginOffset) {
                consumer.seek(tp, aimOffset);
                System.err.println("偏移量—>移动成功: " + tp + "|" + aimOffset);
            } else {
                consumer.seek(tp, beginOffset);
                System.err.println("移动失败->并且移动至起始位置:" + tp + "|" + aimOffset + "|" + beginOffset + "|" + endOffset);
            }
        }
        consumer.commitSync();

        ConsumerUtil.collect(TOPIC, groupId);
    }



    public static Properties initConfig() {

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

}
