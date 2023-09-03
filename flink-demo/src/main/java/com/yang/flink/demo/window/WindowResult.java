package com.yang.flink.demo.window;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

/**
 * 自定义WindowFunction,实现如何收集窗口结果数据
 * interface WindowFunction<IN, OUT, KEY, W extends Window>
 * interface WindowFunction<Double, CategoryPojo, Tuple的真实类型就是String就是分类, W extends Window>
 */
public class WindowResult implements WindowFunction<Double, CategoryPojo, Tuple, TimeWindow> {
    //定义一个时间格式化工具用来将当前时间(双十一那天订单的时间)转为String格式
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
        String category = ((Tuple1<String>) tuple).f0;

        Double price = input.iterator().next();
        //为了后面项目铺垫,使用一下用Bigdecimal来表示精确的小数
        BigDecimal bigDecimal = new BigDecimal(price);
        //setScale设置精度保留2位小数,
        double roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();//四舍五入

        long currentTimeMillis = System.currentTimeMillis();
        String dateTime = df.format(currentTimeMillis);

        CategoryPojo categoryPojo = new CategoryPojo(category, roundPrice, dateTime);
        out.collect(categoryPojo);
    }
}