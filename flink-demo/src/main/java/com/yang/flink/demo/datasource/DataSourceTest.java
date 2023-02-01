package com.yang.flink.demo.datasource;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

/**
 * @author yangliangchuang 2022/10/27 9:00
 * (1)flink 学习（三）mysql 作为数据源
 * https://blog.csdn.net/RenshenLi/article/details/123972210
 * (2)Flink自定义DataSource之MysqlSource
 * https://blog.csdn.net/a_drjiaoda/article/details/89187150
 */
public class DataSourceTest {

    @Test
    public void fromMysqlTest() throws Exception {
        // flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置模式 STREAMING
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //添加mysql数据源
        DataStreamSource<UserInfo> source = env.addSource(new MysqlRichParallelSource());
        //打印结果
        source.print();
        //开始执行
        env.execute("flink streaming from mysql");
    }

    @Test
    public void fromMysqlTest2() throws Exception {
        // flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置模式 STREAMING
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //添加mysql数据源
        DataStreamSource<UserInfo> source = env.addSource(new MysqlRichSourceFunction());
        //打印结果
        source.print();
//        source.print().setParallelism(2);;
        //开始执行
        env.execute("flink streaming from mysql");
    }

}
