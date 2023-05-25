package com.yang.flink.demo.groovy.load;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import groovy.lang.GroovyClassLoader;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LoadCustomProcessFunction {
    public static void main(String[] args) throws Exception {
        // 创建 GroovyClassLoader 实例
        GroovyClassLoader classLoader = new GroovyClassLoader();

        byte[] scriptBytes = Files.readAllBytes(Paths.get("D:\\ylc\\code\\type-it-up-bd\\flink-demo\\src\\main\\java\\com\\yang\\flink\\demo\\groovy\\script\\split.groovy"));
        String splitScript = new String(scriptBytes, StandardCharsets.UTF_8);

        // 读取 Groovy 脚本文件内容
        String scriptContent = splitScript;
        
        // 编译 Groovy 脚本文件并加载类
        Class<?> processFunctionClass = classLoader.parseClass(scriptContent);
        
        // 创建 ProcessFunction 实例
        ProcessFunction<String, String> processFunction = (ProcessFunction<String, String>) processFunctionClass.newInstance();
        
        // 使用 ProcessFunction 处理数据流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.fromElements("Hello", "World");
        DataStream<String> outputStream = inputStream.process(processFunction);
        
        // 输出结果
        outputStream.print();
        
        // 执行任务
        env.execute("Load Custom ProcessFunction");
    }
}
