package com.yang.flink.demo.operator.split;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;

/**
 * https://www.jianshu.com/p/4a39bcf85888
 */
public class OutOfOrderDemo {
    // 创建tag
    public static final OutputTag<Tuple2<String, Integer>> lateTag = new OutputTag<Tuple2<String, Integer>>("late"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 示例数据，其中D乱序，I来迟（H到来的时候认为15000ms之前的数据已经到齐）
        SingleOutputStreamOperator<Tuple2<String, Integer>> source = executionEnvironment.fromElements(
                new Tuple2<>("A", 0),
                new Tuple2<>("B", 1000),
                new Tuple2<>("C", 2000),
                new Tuple2<>("D", 7000),
                new Tuple2<>("E", 3000),
                new Tuple2<>("F", 4000),
                new Tuple2<>("G", 5000),
                new Tuple2<>("H", 20000),
                new Tuple2<>("I", 8000)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forGenerator(new WatermarkGeneratorSupplier<Tuple2<String, Integer>>() {
            // 这里自定义WatermarkGenerator的原因是Flink按照运行时间周期发送watermark，但我们的例子是单次执行的，可以认为数据是一瞬间到来
            // 因此我们改写为每到来一条数据发送一次watermark，watermark的时间戳为数据的事件事件减去5000毫秒，意思是最多容忍数据来迟5000毫秒
            @Override
            public WatermarkGenerator<Tuple2<String, Integer>> createWatermarkGenerator(Context context) {
                return new WatermarkGenerator<Tuple2<String, Integer>>() {
                    @Override
                    public void onEvent(Tuple2<String, Integer> event, long eventTimestamp, WatermarkOutput output) {
                        long watermark = eventTimestamp - 5000L < 0 ? 0L : eventTimestamp - 5000L;
                        output.emitWatermark(new Watermark(watermark));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {

                    }
                };
            }
        // 取第二个字段为watermark
        }).withTimestampAssigner((element, timestamp) -> element.f1));

        // 窗口大小5秒，允许延迟5秒
        // watermark和allowedLateness的区别是，watermark决定了什么时候窗口数据触发计算，allowedLateness决定什么数据被认为是lateElement，从而发送到sideOutput
        // 设置side output tag
        source.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(5)).sideOutputLateData(lateTag).process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Object> out) throws Exception {
                Iterator<Tuple2<String, Integer>> iterator = elements.iterator();
                System.out.println("--------------------");
                while(iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
            }
        // 打印sideoutput流内容
        }).getSideOutput(lateTag).process(new ProcessFunction<Tuple2<String, Integer>, Object>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, ProcessFunction<Tuple2<String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                System.out.println("Late element: " + value);
            }
        });

        executionEnvironment.execute();
    }
}