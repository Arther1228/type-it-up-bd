package com.yang.flink.demo.operator.split;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * https://www.jianshu.com/p/4a39bcf85888
 */
public class SplitDemo {
    public static final OutputTag<Integer> evenTag = new OutputTag<Integer>("even"){};
    public static final OutputTag<Integer> oddTag = new OutputTag<Integer>("odd"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = executionEnvironment.fromElements(1, 2, 3, 4, 5);

        SingleOutputStreamOperator<Integer> process = source.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    // 这里不使用out.collect，而是使用ctx.output
                    // 这个方法多了一个参数，可以指定output tag，从而实现数据分流
                    ctx.output(evenTag, value);
                } else {
                    ctx.output(oddTag, value);
                }
            }
        });

        // 依赖OutputTag获取对应的旁路输出
        DataStream<Integer> evenStream = process.getSideOutput(evenTag);
        DataStream<Integer> oddStream = process.getSideOutput(oddTag);

        // 分别打印两个旁路输出流中的数据
        evenStream.process(new ProcessFunction<Integer, String>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("Even: " + value);
            }
        }).print();

        oddStream.process(new ProcessFunction<Integer, String>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("Odd: " + value);
            }
        }).print();

        executionEnvironment.execute();
    }
}