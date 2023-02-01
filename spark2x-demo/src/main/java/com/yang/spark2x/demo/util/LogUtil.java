package com.yang.spark2x.demo.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.suncreate.logback.elasticsearch.metric.*;
import com.suncreate.logback.elasticsearch.util.MetricUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;


/**
 * @Author He Kun
 * @Date 2019/9/3 14:27
 */
public class LogUtil {

    private static final Logger log = LoggerFactory.getLogger(LogUtil.class);

    public static String clearFailLog(String errorMsg) {
        String json=null;
        HashMap<String, Object> logMap = (HashMap<String, Object>) MetricUtil.getMap(
                DataCatalog.vehicle.toString(),
                "ShinyPassingVehicle",
                DataType.struct_data.toString(),
                ProcPhase.clear.toString(),
                ProcStatus.fail.toString(),
                SourceType.kafka.toString(),
                SinkType.kafka.toString(),1);
        logMap.put("Error",errorMsg);
        ObjectMapper mapper = new ObjectMapper();
        try {
            // convert map to JSON string
            json = mapper.writeValueAsString(logMap);
        } catch (JsonProcessingException e) {
            log.error("{\"JsonProcessingException\": \""+e.toString()+"\"}");
        }
        return json;
    }

    public static String clearSucLog() {
        String json=null;
        HashMap<String, Object> logMap = (HashMap<String, Object>) MetricUtil.getMap(
                DataCatalog.vehicle.toString(),
                "ShinyPassingVehicle",
                DataType.struct_data.toString(),
                ProcPhase.clear.toString(),
                ProcStatus.suc.toString(),
                SourceType.kafka.toString(),
                SinkType.kafka.toString(),1);
        ObjectMapper mapper = new ObjectMapper();
        try {
            // convert map to JSON string
            json = mapper.writeValueAsString(logMap);
        } catch (JsonProcessingException e) {
            log.error("{\"JsonProcessingException\": \""+e.toString()+"\"}");
        }
        return json;
    }


    public static String generateSucLog(String dataSource) {
        String json=null;
        HashMap<String, Object> logMap = (HashMap<String, Object>) MetricUtil.getMap(
                DataCatalog.vehicle.toString(),
                "ShinyPassingVehicle",
                DataType.struct_data.toString(),
                "generate",
                ProcStatus.suc.toString(),
                SourceType.kafka.toString(),
                SinkType.kafka.toString(), 1);
        logMap.put("data_source_sub", dataSource);
        ObjectMapper mapper = new ObjectMapper();
        try {
            // convert map to JSON string
            json = mapper.writeValueAsString(logMap);
        } catch (JsonProcessingException e) {
            log.error("{\"JsonProcessingException\": \""+e.toString()+"\"}");
        }
        return json;
    }


    public static String collectSucLog(String dataSource) {
        String json=null;
        HashMap<String, Object> logMap = (HashMap<String, Object>) MetricUtil.getMap(
                DataCatalog.vehicle.toString(),
                "ShinyPassingVehicle",
                DataType.struct_data.toString(),
                ProcPhase.collect.toString(),
                ProcStatus.suc.toString(),
                SourceType.kafka.toString(),
                SinkType.kafka.toString(), 1);
        logMap.put("data_source_sub", dataSource);
        ObjectMapper mapper = new ObjectMapper();
        try {
            // convert map to JSON string
            json = mapper.writeValueAsString(logMap);
        } catch (JsonProcessingException e) {
            log.error("{\"JsonProcessingException\": \""+e.toString()+"\"}");
        }
        return json;
    }

    public static String dirtyDataInfoLog(String dataSource, String incorrectTime, String tollgateName) {
        String json=null;
        HashMap<String, String> dirtyDataInfo = new HashMap<>(4);
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        dirtyDataInfo.put("Time", time);
        dirtyDataInfo.put("IncorrectPassTime", incorrectTime);
        dirtyDataInfo.put("TollgateName", tollgateName);
        dirtyDataInfo.put("Datasource", dataSource);
        ObjectMapper mapper = new ObjectMapper();
        try {
            // convert map to JSON string
            json = mapper.writeValueAsString(dirtyDataInfo);
        } catch (JsonProcessingException e) {
            log.error("{\"JsonProcessingException\": \""+e.toString()+"\"}");
        }
        return json;
    }

    public static String exceptionLog(String contents, String remarks) {
        String json=null;
        HashMap<String, String> exception = new HashMap<>(4);
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        exception.put("Time", time);
        exception.put("Contents",contents);
        exception.put("Remarks", remarks);
        ObjectMapper mapper = new ObjectMapper();
        try {
            // convert map to JSON string
            json = mapper.writeValueAsString(exception);
        } catch (JsonProcessingException e) {
            log.error("{\"JsonProcessingException\": \""+e.toString()+"\"}");
        }
        return json;
    }

}
