package com.yang.flink.demo.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 自定义价格聚合函数,其实就是对price的简单sum操作
 * AggregateFunction<IN, ACC, OUT>
 * AggregateFunction<Tuple2<String, Double>, Double, Double>
 */
public class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
    //初始化累加器为0
    @Override
    public Double createAccumulator() {
        return 0D; //D表示Double,L表示long
    }

    //把price往累加器上累加
    @Override
    public Double add(Tuple2<String, Double> value, Double accumulator) {
        return value.f1 + accumulator;
    }

    //获取累加结果
    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    //各个subTask的结果合并
    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}