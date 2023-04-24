package com.yang.kafka.demo.offset;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author yangliangchuang 2022/9/5 18:43
 *  * 尝试使用时间戳完成偏移量设置
 *  * https://blog.csdn.net/weixin_38251332/article/details/120081411
 */
public class SetTimestampOffsetConsumerDemo {

    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    private final static String TOPIC = "motorVehicleDisposition";

    public static void setOffset(Consumer<String, String> consumer, long timestamp, String topic) {
        //===========================
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
        Instant instant = Instant.ofEpochMilli(timestamp);
        String format = formatter.format(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));

        //时间戳设置
//        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        /*这两个方法需要绑定使用，否则consumer.assignment()获取的数据为空
        consumer.assign(Arrays.asList(new TopicPartition("t7", 2)));
        Set<TopicPartition> partitionInfos = consumer.assignment();*/
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
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
        //=========================


        int count = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    System.out.println(record.offset() + ":" + record.partition() + ":" + record.timestamp());
                }
                Thread.sleep(1);

                if (count % 4 == 0) {
                    consumer.commitSync();
                    count = 0;
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {

        Consumer<String, String> consumer = Commons.createConsumer();

        setOffset(consumer, 1662384147000L, TOPIC);
    }

}
