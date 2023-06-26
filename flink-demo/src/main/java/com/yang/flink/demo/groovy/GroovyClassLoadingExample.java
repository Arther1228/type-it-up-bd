package com.yang.flink.demo.groovy;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

/**
 * 报错示例
 */
public class GroovyClassLoadingExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Field configuration = StreamExecutionEnvironment.class.getDeclaredField("configuration");
        configuration.setAccessible(true);
        Configuration o = (Configuration) configuration.get(env);

        Field confData = Configuration.class.getDeclaredField("confData");
        confData.setAccessible(true);
        Map<String, Object> temp = (Map<String, Object>) confData.get(o);

        // 创建自定义的Groovy类，并编译成字节码
        String className = "MyMapFunction";
        String groovyCode = "class " + className + " implements MapFunction<String, Integer> {\n" +
                "\t@Override\n" +
                "\tpublic Integer map(String value) throws Exception {\n" +
                "\t\treturn value.length();\n" +
                "\t}\n" +
                "}";
        CompilerConfiguration config = new CompilerConfiguration();
        ImportCustomizer importCustomizer = new ImportCustomizer();
        importCustomizer.addStarImports("org.apache.flink.api.common.functions");
        config.addCompilationCustomizers(importCustomizer);
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        URLClassLoader classLoader = new URLClassLoader(new URL[0], parent);
        Class<?> groovyClass = GroovyUtils.compile(groovyCode, className, config, classLoader);
        temp.put("pipeline.jars", groovyClass);

        // 使用自定义的Groovy类进行计算
        DataStreamSource<String> input = env.fromElements("hello", "world");
        SingleOutputStreamOperator<Integer> output = input.map((MapFunction<String, Integer>) groovyClass.newInstance());
        output.print();

        JobExecutionResult result = env.execute();
    }
}
