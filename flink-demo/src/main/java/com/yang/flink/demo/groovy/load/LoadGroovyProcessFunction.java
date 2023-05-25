package com.yang.flink.demo.groovy.load;

import groovy.util.GroovyScriptEngine;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public class LoadGroovyProcessFunction {
    public static void main(String[] args) throws Exception {
        // 创建 GroovyScriptEngine 实例
        GroovyScriptEngine engine = new GroovyScriptEngine("D:\\ylc\\code\\type-it-up-bd\\flink-demo\\src\\main\\java\\com\\yang\\flink\\demo\\groovy\\script\\");
        
        // 加载 Groovy 脚本文件
        String scriptName = "split.groovy";
        Class<?> processFunctionClass = engine.loadScriptByName(scriptName);
        
        // 创建 ProcessFunction 实例
        ProcessFunction<String, String> processFunction = (ProcessFunction<String, String>) processFunctionClass.newInstance();
        
        // 使用 ProcessFunction 处理数据流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.fromElements("Hello", "World");
        DataStream<String> outputStream = inputStream.process(processFunction);
        
        // 输出结果
        outputStream.print();
        
        // 执行任务
        env.execute("Load Groovy ProcessFunction");
    }
}
