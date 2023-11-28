package com.yang.kafka.demo.lag;

import com.yang.kafka.demo.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yangliangchuang 2023/11/28 8:28
 */
@Slf4j
public class TopicOffsetSearch {

    private final static String topic = "KK-JJ";

    private final static KafkaConsumer consumer = (KafkaConsumer) KafkaUtil.createEmptyConsumer(KafkaUtil.getShinyClusterServer());

    /**
     * 查询最后的offset
     *
     * @return
     */
    public Map<Integer, Long> searchEndOffset() {

        //查询 topic partitions
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfoList) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitionList.add(topicPartition);
        }

        //查询 log size
        Map<Integer, Long> endOffsetMap = new HashMap<>();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitionList);
        for (TopicPartition partitionInfo : endOffsets.keySet()) {
            endOffsetMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));
            log.info("partition index: {} , endOffsets :{}", partitionInfo.partition(), endOffsets.get(partitionInfo));
        }

        return endOffsetMap;
    }

    /**
     * 查询最早的offset
     *
     * @return
     */
    public Map<Integer, Long> searchBeginOffset() {

        //查询 topic partitions
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfoList) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitionList.add(topicPartition);
        }

        //查询 log size
        Map<Integer, Long> beginningOffsetMap = new HashMap<>();
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitionList);
        for (TopicPartition partitionInfo : beginningOffsets.keySet()) {
            beginningOffsetMap.put(partitionInfo.partition(), beginningOffsets.get(partitionInfo));
            log.info("partition index: {} , beginningOffsets :{}", partitionInfo.partition(), beginningOffsets.get(partitionInfo));
        }
        return beginningOffsetMap;
    }

    @Test
    public void checkDataSkew() {

        Map<Integer, Long> endOffsetMap = searchEndOffset();

        // 计算各个key的value是否平均
        boolean isAverage = checkAverage(endOffsetMap);

        if (isAverage) {
            System.out.println("各个分区的offset值相差不超过10%，数据未产生明显倾斜。");
        } else {
            System.out.println("各个分区的offset值相差超过10%，数据倾斜。");
        }
    }

    /**
     * 检查各个key的value是否平均
     */
    private static boolean checkAverage(Map<Integer, Long> map) {
        // 计算所有value的平均值
        long sum = 0;
        for (long value : map.values()) {
            sum += value;
        }
        double average = (double) sum / map.size();

        // 判断各个value是否在平均值的范围内
        for (long value : map.values()) {
            if (Math.abs(value - average) > 0.1 * average) {
                return false;
            }
        }
        return true;
    }

    /**
     * 累加当前topic的数据量
     */
    @Test
    public void countTopicData() {
        Map<Integer, Long> beginningOffsetMap = searchBeginOffset();

        Map<Integer, Long> endOffsetMap = searchEndOffset();

        if (beginningOffsetMap.size() != endOffsetMap.size()) {
            log.error("分区查询失败");
            return;
        }

        Map<Integer, Long> partitionDataMap = new HashMap<>();

        for (Integer key : beginningOffsetMap.keySet()) {
            long beginOffset = beginningOffsetMap.get(key);
            long endOffset = endOffsetMap.get(key);
            long count = endOffset - beginOffset;
            partitionDataMap.put(key, count);
            log.info("partition index: {} , 数据量count :{}", key, count);
        }

        long sum = 0;
        for (long value : partitionDataMap.values()) {
            sum += value;
        }

        log.info("当前kafka共有数据量：{}", sum);

    }
}
