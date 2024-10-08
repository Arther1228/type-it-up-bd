package com.yang.flink.demo.groovy.operator;

import com.yang.flink.demo.groovy.util.LoadGroovyClassUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class GrooyJSONObjectFiltter {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer("access_authority", new SimpleStringSchema(), properties);
        DataStream<String> kafkaStream = env.addSource(kafkaSource);

        byte[] scriptBytes = Files.readAllBytes(Paths.get("D:\\ylc\\code\\type-it-up-bd\\flink-demo\\src\\main\\java\\com\\yang\\flink\\demo\\groovy\\script\\JSONObject-filter.groovy"));
        String groovyScript = new String(scriptBytes, StandardCharsets.UTF_8);

        // 编译 Groovy 脚本文件并加载类
        Class<?> filterFunctionClass = LoadGroovyClassUtil.parseClass(groovyScript);
        // 创建 MapFunction 实例
        FilterFunction<String> filterFunction = (FilterFunction<String>) filterFunctionClass.newInstance();
        DataStream<String> filterStream = kafkaStream.filter(filterFunction);
        filterStream.print();

        env.execute("GroovyFilter Job");
    }
}
