package com.yang.kafka.demo.offset;

import com.yang.kafka.demo.util.ConsumerUtil;
import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 消费最新的offset数据示例
 * https://www.cnblogs.com/cocowool/p/get_kafka_latest_offset.html
 */

public class SetBeginOrEndOffset {

    private final static String TOPIC = "motorVehicleDisposition";
    private final static String groupId = "test1";

    final static Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);
    final static Collection<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);

    /**
     * 获取某个Topic的所有分区以及分区最新的Offset
     */
    @Test
    public void setBeginOffset() {
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

        ConsumerUtil.collect(TOPIC, groupId);
    }

    @Test
    public void setEndOffset() {
        System.out.println("Get the partition info as below:");
        List<TopicPartition> tp = new ArrayList<TopicPartition>();
        partitionInfos.forEach(str -> {
            System.out.println("Partition Info:");
            System.out.println(str);
            tp.add(new TopicPartition(TOPIC, str.partition()));
//            consumer.assign(tp);
            consumer.seekToBeginning(tp);
            System.out.println("Partition " + str.partition() + " 's latest offset is '" + consumer.position(new TopicPartition(TOPIC, str.partition())));
        });

        ConsumerUtil.collect(TOPIC, groupId);
    }

}
