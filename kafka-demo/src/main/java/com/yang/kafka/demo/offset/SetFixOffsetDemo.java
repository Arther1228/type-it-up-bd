package com.yang.kafka.demo.offset;

import com.yang.kafka.demo.util.ConsumerUtil;
import com.yang.kafka.demo.util.KafkaUtil;
import com.yang.kafka.demo.util.PartitionUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;

/**
 * @author yangliangchuang 2022/9/6 13:28
 */

public class SetFixOffsetDemo {

    private final static String TOPIC = "motorVehicleDisposition";
    private final static String groupId = "test3";
    final static Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);

    /**
     * 固定offset
     * https://www.cnblogs.com/caoweixiong/p/11684370.html
     */
    @Test
    public void setFixOffset() {
        Set<TopicPartition> assignment = PartitionUtil.getAssignment(consumer, TOPIC);
        for (TopicPartition tp : assignment) {
            int offset = 12258846;
            System.out.println("分区 " + tp + " 从 " + offset + " 开始消费");
            consumer.seek(tp, offset);
        }
        ConsumerUtil.pollRecords(consumer);
    }

    /**
     * @return https://blog.csdn.net/jj89929665/article/details/121681927
     * 【效果比较好】
     */
    @Test
    public void reduceFixOffset() {

        int target = 20000;
        System.err.println("希望偏移量参数位置为: " + target);

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("获取订阅-开始拉去数据");
        ConsumerRecords<String, String> records = consumer.poll(1000);
        System.err.println("偏移量记录位置为: " + records.count());

        //查询 topic partitions
        List<TopicPartition> partitionList = PartitionUtil.getTopicPartitionList((KafkaConsumer) consumer, TOPIC);

        Map<TopicPartition, Long> endMap = consumer.endOffsets(partitionList);
        Map<TopicPartition, Long> beginmap = consumer.beginningOffsets(partitionList);

        long aimOffset;
        for (TopicPartition tp : partitionList) {
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

        ConsumerUtil.pollRecords(consumer);
    }

}
