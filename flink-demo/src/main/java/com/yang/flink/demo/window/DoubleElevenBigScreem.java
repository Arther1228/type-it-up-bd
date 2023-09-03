package com.yang.flink.demo.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

/**
 * Author oldlu
 * Desc
 * 模拟双11商品实时交易大屏统计分析
 * https://www.cnblogs.com/zhaoguanglu/p/15653002.html
 */
public class DoubleElevenBigScreem {
    public static void main(String[] args) throws Exception {
        //编码步骤:
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//学习测试方便观察

        //2.source
        //模拟实时订单信息
        DataStreamSource<Tuple2<String, Double>> sourceDS = env.addSource(new MySource());

//        sourceDS.print();

        /*
        注意:需求如下：
        -1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额
        -2.计算出各个分类的销售额top3
        -3.每1秒钟更新一次统计结果
        如果使用之前学习的简单的timeWindow(Time size窗口大小, Time slide滑动间隔)来处理,
        如xxx.timeWindow(24小时,1s),计算的是需求中的吗?
        不是!如果使用之前的做法那么是完成不了需求的,因为:
        如11月11日00:00:01计算的是11月10号[00:00:00~23:59:59s]的数据
        而我们应该要计算的是:11月11日00:00:00~11月11日00:00:01
        所以不能使用之前的简单做法!*/

        //3.transformation
        //.keyBy(0)
        SingleOutputStreamOperator<CategoryPojo> tempAggResult = sourceDS.keyBy(0)
                //3.1定义大小为一天的窗口,第二个参数表示中国使用的UTC+08:00时区比UTC时间早
                /*
                of(Time 窗口大小, Time 带时间校准的从哪开始)源码中有解释:
                如果您居住在不使用UTC±00：00时间的地方，例如使用UTC + 08：00的中国，并且您需要一个大小为一天的时间窗口，
                并且窗口从当地时间的每00:00:00开始，您可以使用of(Time.days(1)，Time.hours(-8))
                注意:该代码如果在11月11日运行就会从11月11日00:00:00开始记录直到11月11日23:59:59的1天的数据
                注意:我们这里简化了没有把之前的Watermaker那些代码拿过来,所以直接ProcessingTime
                */
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))//仅仅只定义了一个窗口大小
                //3.2定义一个1s的触发器
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                //上面的3.1和3.2相当于自定义窗口的长度和触发时机
                //3.3聚合结果.aggregate(new PriceAggregate(), new WindowResult());
                //.sum(1)//以前的写法用的默认的聚合和收集
                //现在可以自定义如何对price进行聚合,并自定义聚合结果用怎样的格式进行收集
                .aggregate(new PriceAggregate(), new WindowResult());
        //3.4看一下初步聚合的结果
        tempAggResult.print("初步聚合结果");
        //CategoryPojo(category=运动, totalPrice=118.69, dateTime=2020-10-20 08:04:12)
        //上面的结果表示:当前各个分类的销售总额

        /*
        注意:需求如下：
        -1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额
        -2.计算出各个分类的销售额top3
        -3.每1秒钟更新一次统计结果
         */
        //4.使用上面初步聚合的结果,实现业务需求,并sink
        tempAggResult.keyBy("dateTime")//按照时间分组是因为需要每1s更新截至到当前时间的销售总额
                //每秒钟更新一次统计结果
                //Time size 为1s,表示计算最近1s的数据
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                //在ProcessWindowFunction中实现该复杂业务逻辑,一次性将需求1和2搞定
                //-1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额
                //-2.计算出各个分类的销售额top3
                //-3.每1秒钟更新一次统计结果
                .process(new WindowResultProcess());//window后的process方法可以处理复杂逻辑

        //5.execute
        env.execute();
    }

}

