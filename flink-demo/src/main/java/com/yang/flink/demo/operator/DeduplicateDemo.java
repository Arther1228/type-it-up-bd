package com.yang.flink.demo.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class DeduplicateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入数据流
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 8888);

        // 数据去重并定期清空 HashSet
        DataStream<Tuple2<String, Integer>> distinctDataStream = inputDataStream
                .keyBy(value -> value)
                .process(new KeyedProcessFunction<String, String, Tuple2<String, Integer>>() {
                    private HashSet<String> minuteDataFilter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        minuteDataFilter = new HashSet<>(10000);
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) {
                        if (!minuteDataFilter.contains(value)) {
                            minuteDataFilter.add(value);
                            out.collect(Tuple2.of(value, 1));
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        minuteDataFilter.clear();
                        super.onTimer(timestamp, ctx, out);
                    }


                    @Override
                    public void close() throws Exception {
                        minuteDataFilter = null;
                        super.close();
                    }
                });

        // 输出结果
        distinctDataStream.print();

        env.execute("Flink Demo");
    }
}
