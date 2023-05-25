package com.yang.flink.demo.groovy.operator;

import com.yang.flink.demo.groovy.load.LoadGroovyClassUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.OutputTag;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author HP
 */
public class GroovySplitter {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        byte[] scriptBytes = Files.readAllBytes(Paths.get("D:\\ylc\\code\\type-it-up-bd\\flink-demo\\src\\main\\java\\com\\yang\\flink\\demo\\groovy\\script\\split.groovy"));
        String groovyScrpit = new String(scriptBytes, StandardCharsets.UTF_8);

        // Kafka配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("access_authority", new SimpleStringSchema(), properties);
        DataStream<String> kafkaStream = env.addSource(kafkaSource);

        // 编译 Groovy 脚本文件并加载类
        Class<?> processFunctionClass = LoadGroovyClassUtil.parseClass(groovyScrpit);
        // 创建 ProcessFunction 实例
        ProcessFunction<String, String> processFunction = (ProcessFunction<String, String>) processFunctionClass.newInstance();

        // 获取 outputTags 数组
        Field outputTagsField = processFunctionClass.getDeclaredField("outputTags");
        outputTagsField.setAccessible(true);
        ArrayList<OutputTag> outputTags = (ArrayList<OutputTag>) outputTagsField.get(processFunction);

        outputTags.stream().forEach(outputTag -> {
            DataStream<String> tagStream = kafkaStream.process(processFunction).getSideOutput(outputTag);
            if (outputTag.getId().equals("topic-B")) {
                tagStream.print();
            }
        });

        // 将分流后的两个流输出到不同的Kafka主题中
//        FlinkKafkaProducer<String> kafkaSink1 = new FlinkKafkaProducer<>("even-topic", new SimpleStringSchema(), properties);
//        evenStream.addSink(kafkaSink1);
//
//        FlinkKafkaProducer<String> kafkaSink2 = new FlinkKafkaProducer<>("odd-topic", new SimpleStringSchema(), properties);
//        oddStream.addSink(kafkaSink2);

        env.execute("GroovySplitter Job");
    }
}
