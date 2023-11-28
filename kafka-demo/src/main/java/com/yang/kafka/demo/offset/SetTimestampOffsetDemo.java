package com.yang.kafka.demo.offset;

import com.yang.kafka.demo.util.ConsumerUtil;
import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author yangliangchuang 2022/9/5 18:43
 * * 尝试使用时间戳完成偏移量设置
 */
public class SetTimestampOffsetDemo {

    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    private final static String TOPIC = "motorVehicleDisposition";
    private final static String groupId = "test1";

    final static Consumer<String, String> consumer = KafkaUtil.createConsumer(KafkaUtil.getShinyClusterServer(), groupId);

    /**
     * https://blog.csdn.net/weixin_38251332/article/details/120081411
     */
    @Test
    public static void setOffset() {
        long timestamp = 1662384147000L;
        //===========================
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
        Instant instant = Instant.ofEpochMilli(timestamp);
        String format = formatter.format(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));

        //时间戳设置
//        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        /*这两个方法需要绑定使用，否则consumer.assignment()获取的数据为空
        consumer.assign(Arrays.asList(new TopicPartition("t7", 2)));
        Set<TopicPartition> partitionInfos = consumer.assignment();*/
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
        if (null != partitionInfos && partitionInfos.size() > 0) {
            Map<TopicPartition, Long> map = new HashMap<>(8);
            for (PartitionInfo p : partitionInfos) {
                map.put(new TopicPartition(p.topic(), p.partition()), timestamp);
            }

            System.out.println("按照" + format + "最新的偏移量");
            Map<TopicPartition, OffsetAndTimestamp> offsetTimestamp = consumer.offsetsForTimes(map);
            List<TopicPartition> tp = new ArrayList<>();
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetTimestamp.entrySet()) {
                TopicPartition key = entry.getKey();
                OffsetAndTimestamp value = entry.getValue();

                tp.add(key);
                consumer.assign(tp);

                //根据消费里的timestamp确定offset
                long position = 0;
                if (value != null) {
                    position = value.offset();
                } else {
                    //当指定时间戳大于最分区最新数据时间戳时，为null
//                    consumer.seekToEnd(Collections.singleton(key));
                    consumer.seekToBeginning(Collections.singleton(key));
                    position = consumer.position(key);
                }
                consumer.seek(key, position);
            }

        }
        //时间戳设置完毕

        ConsumerUtil.collect(TOPIC, groupId);
    }

    /**
     * https://www.cnblogs.com/caoweixiong/p/11684370.html
     */
    @Test
    public void setTimeStampOffset() {

        Properties props = SetFixOffsetDemo.initConfig();
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

        ConsumerUtil.collect(TOPIC, groupId);
    }

}
