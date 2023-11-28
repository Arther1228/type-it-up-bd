package com.yang.kafka.demo.lag;

import com.yang.kafka.demo.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.*;

/**
 *
 */
@Slf4j
public class TopicLagSearch {

    private final static String topic = "wifiData";
    private final static String groupId = "wifi-kafka-hbase";

    /**
     * @return
     */
    @Test
    public void getConsumerLag() {
        long startTime = System.currentTimeMillis();

        KafkaConsumer consumer = (KafkaConsumer) KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);

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
        }

        //查询消费 offset
        Map<Integer, Long> commitOffsetMap = new HashMap<>();
        for (TopicPartition topicAndPartition : topicPartitionList) {
            OffsetAndMetadata committed = consumer.committed(topicAndPartition);
            commitOffsetMap.put(topicAndPartition.partition(), committed.offset());
        }

        long endTime = System.currentTimeMillis();
        log.info("Topic:" + topic + "  groupId:" + groupId + "  查询logSize和offset耗时:" + (new DecimalFormat("0.000")).format((endTime - startTime) / 1000.0) + " 秒");

        //累加lag
        long totalLag = 0L;
        long logSize = 0L;
        long offset = 0L;
        if (endOffsetMap.size() == commitOffsetMap.size()) {
            for (Integer partition : endOffsetMap.keySet()) {
                long endOffset = endOffsetMap.get(partition);
                long commitOffset = commitOffsetMap.get(partition);
                long diffOffset = endOffset - commitOffset;
                totalLag += diffOffset;
                logSize += endOffset;
                offset += commitOffset;
            }

        } else {
            log.error("Topic:" + topic + "  groupId:" + groupId + "  topic partitions lost");
        }

        log.info("Topic:" + topic + "  groupId:" + groupId + "  logSize:" + logSize + "  offset:" + offset + "  totalLag:" + totalLag);
    }

}

