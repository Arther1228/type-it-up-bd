package com.yang.flink.demo.map;

import com.yang.flink.demo.datasource.MysqlRichSourceFunction;
import com.yang.flink.demo.datasource.UserInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yangliangchuang 2022/10/27 11:58
 * flink 实现CommonSink，自动生成sql，自动匹配字段值
 * https://blog.csdn.net/devcy/article/details/116023780
 */
public class SinkTest {

    public static void main(String[] args) throws Exception {

        String tn = "user_info2";

        /**读取数据**/
        // flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置模式 STREAMING
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //添加mysql数据源
        DataStreamSource<UserInfo> source = env.addSource(new MysqlRichSourceFunction());
        /** 中间可以进行一些数据转换操作 **/

        source.name("collect-data").map(new MapFunction<UserInfo, UserInfo>() {
            @Override
            public UserInfo map(UserInfo userInfo) {
                String newName = userInfo.getUserRealName() + "_1";
                userInfo.setUserRealName(newName);
                return userInfo;
            }
        }).name("map-data").addSink(new CommonSink(tn)).name("sink data");

        /**sink数据**/
        env.execute("clean data task");
        System.out.println("task is finished.");
    }
}
