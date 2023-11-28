package com.yang.kafka.demo.offset;

import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author JMWANG
 * 根据固定的offset差值设置偏移量，执行方法在测试包下 OffsetTest.java
 * https://blog.csdn.net/jj89929665/article/details/121681927
 */
public class ReduceFixOffset {

    private final static String TOPIC = "motorVehicleDisposition";
    private final static String groupId = "test1";

    /**
     * @return 返回kafkaProducer对象进行操作
     */
    public static void seekTopicData(int target) {
        final Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);

        final String topicall = TOPIC;
        String[] topics = topicall.split(",");

        for (String topic : topics) {
            consumer.subscribe(Arrays.asList(topic.split(",")));
            System.out.println("获取订阅-开始拉去数据");
            ConsumerRecords<String, String> records = consumer.poll(10000);
            System.err.println("偏移量记录位置为: " + records.count());
            System.err.println("希望偏移量参数位置为: " + target);

            List<PartitionInfo> list = consumer.partitionsFor(topic);
            System.err.println(topic + " 主题 的分区数为:" + list.size());

            List<TopicPartition> topicList = new ArrayList<TopicPartition>();

            for (PartitionInfo pt : list) {
                TopicPartition tp = new TopicPartition(topic, pt.partition());

                topicList.add(tp);
            }
            Map<TopicPartition, Long> endMap = consumer.endOffsets(topicList);
            Map<TopicPartition, Long> beginmap = consumer.beginningOffsets(topicList);

            int i = 0;
            long aimOffset = 0;
            for (TopicPartition tp : topicList) {
                System.err.println("消费者为" + tp);
                long endOffset = endMap.get(tp);
                long beginOffset = beginmap.get(tp);

                aimOffset = endOffset - target;

                i++;
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
        }
    }

    public static void main(String[] args) {
        int target = 1;
        seekTopicData(target);
    }

}

