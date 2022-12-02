package com.suncreate.log2esdemo.task;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.suncreate.logback.elasticsearch.metric.DataCatalog;
import com.suncreate.logback.elasticsearch.metric.DataType;
import com.suncreate.logback.elasticsearch.metric.SinkType;
import com.suncreate.logback.elasticsearch.metric.SourceType;
import com.suncreate.logback.elasticsearch.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author admin
 */
public class TimerTaskTest {

    public static void main(String[] args) throws ParseException {
        //创建定时任务
        TaskTest taskTest1 = new TaskTest("taskTest1");
        //定时器创建
        Timer timer = new Timer("定时器线程");
        //指定开始执行的日期
        Date startDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2019-03-03 10:55:10");
        //时间间隔，单位是ms
        long intervalTime = 2 * 1000;
        try {
            timer.schedule(taskTest1, startDate, intervalTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class TaskTest extends TimerTask {

    private static final Logger log = LoggerFactory.getLogger(TaskTest.class);
    /**
     * 测试任务名称
     */
    private String taskTestName;

    public TaskTest(String taskTestName) {
        super();
        this.taskTestName = taskTestName;
    }

    @Override
    public void run() {
        //这里是需要执行的具体任务
        //log.info("定时任务{}执行中，响铃响一下", taskTestName);

        //TODO 不能打印除了固定格式的json String
        Map<String, Object> hashMap = new HashMap<>(6);
        hashMap.put("info", "Upload device image failed.");
        hashMap.put("result", 0);
        hashMap.put("total_time", 1);
        hashMap.put("pid", 12);
        hashMap.put("cameraCode", "340###############");
        hashMap.put("aggCount", 1);
        String message = JSON.toJSONString(hashMap);
        log.info(message);

/*        HashMap<String, Object> logMap = (HashMap<String, Object>) MetricUtil.getMap(DataCatalog.police_data.toString(), "dataSource",
                DataType.struct_data.toString(), "procPhase", "procStatus", SourceType.oracle.toString(),
                SinkType.mpp.toString(), 1);
        log.info(new Gson().toJson(logMap));*/
    }
}