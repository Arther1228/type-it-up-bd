package com.yang.flink.demo.groovy.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * @author HP
 */
public class GroovyDeduplicate {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        byte[] scriptBytes = Files.readAllBytes(Paths.get("D:\\ylc\\code\\type-it-up-bd\\flink-demo\\src\\main\\java\\com\\yang\\flink\\demo\\groovy\\script\\map.groovy"));
        String groovyScript = new String(scriptBytes, StandardCharsets.UTF_8);

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer("access_authority", new SimpleStringSchema(), properties);
        DataStream<String> kafkaStream = env.addSource(kafkaSource);


        env.execute("GroovyDeduplicate Job");
    }
}
