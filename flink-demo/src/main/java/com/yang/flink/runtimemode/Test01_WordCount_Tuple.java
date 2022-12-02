package com.yang.flink.runtimemode;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Flink教程(18) Flink1.12.0 批流一体 setRuntimeMode(RuntimeExecutionMode.BATCH) 问题解决
 * https://blog.csdn.net/winterking3/article/details/115294029
 *
 * flink1.12.x 代码设置流模式、批模式、自动模式
 * https://www.malaoshi.top/show_1IX1z8zbwub0.html
 */
public class Test01_WordCount_Tuple {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //和之前唯一不同的是添加了这行，设置为批处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStreamSource<String> source = env.readTextFile(BaseConstant.WORD_COUNT_TEST);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split(" ");
                        if (split != null && split.length > 0) {
                            Stream<String> stream = Arrays.stream(split);
                            stream.forEach(word -> out.collect(Tuple2.of(word, 1)));
                        }
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne
                .keyBy(f -> f.f0)
                .sum(1);

        sum.print();

        env.execute();
    }
}
