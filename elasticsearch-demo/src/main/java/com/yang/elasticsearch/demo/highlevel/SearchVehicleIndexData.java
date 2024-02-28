package com.yang.elasticsearch.demo.highlevel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class SearchVehicleIndexData {

    public static void main(String[] args) {
        // 设置索引名称和字段
        String plateNo = "皖ADF6520";

        // 连接 Elasticsearch
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("34.8.8.93", 24102, "http"))
        )) {
            // 循环从今天开始，向前推7天
            for (int i = 0; i <= 7; i++) {
                // 获取查询时间范围
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DAY_OF_MONTH, -i);
                Date startOfDay = getStartOfDay(cal.getTime());
                Date endOfDay = getEndOfDay(cal.getTime());

                String indexName = getIndexName(formatDate(startOfDay, "yyyyMMddHHmmss"));
                // 创建查询请求
                SearchRequest searchRequest = new SearchRequest(indexName);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

                // 创建布尔查询
                BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

                // 设置 pass_time 范围查询条件
                boolQuery.must(QueryBuilders.rangeQuery("pass_time")
                        .gte(formatDate(startOfDay, "yyyyMMddHHmmss"))
                        .lt(formatDate(endOfDay, "yyyyMMddHHmmss")));

                // 设置 plate_no 查询条件
                boolQuery.must(QueryBuilders.termQuery("plate_no", plateNo));

                // 将布尔查询设置为主查询条件
                searchSourceBuilder.query(boolQuery);

                // 设置排序规则
                searchSourceBuilder.sort("pass_time", SortOrder.DESC);

                // 设置查询大小为最大值
                searchSourceBuilder.size(10000);

                // 执行查询
                searchRequest.source(searchSourceBuilder);
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

                // 处理查询结果并写入 TXT
                processAndWriteToTxt(searchResponse);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processAndWriteToTxt(SearchResponse searchResponse) {
        // 设置 TXT 文件夹路径
        String txtFolderPath = "result";
        File folder = new File(txtFolderPath);
        // 如果文件夹不存在则创建
        if (!folder.exists()) {
            folder.mkdirs();
        }

        // 创建日期格式化对象
        SimpleDateFormat sdfInput = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            // 获取索引中的字段值
            String passTime = hit.getSourceAsMap().get("pass_time").toString();
            String tollgateName;
            try {
                tollgateName = hit.getSourceAsMap().get("tollgate_name3").toString();
            } catch (Exception e) {
                continue;
            }

            // 解析 passTime 字段值
            try {
                Date date = sdfInput.parse(passTime);

                // 构建保存的 TXT 文件路径
                String txtFilePath = txtFolderPath + "/" + sdfOutput.format(date).substring(0, 10) + ".txt";

                // 写入数据到 TXT 文件
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(txtFilePath, true))) {
                    writer.write(sdfOutput.format(date) + " " + tollgateName);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (java.text.ParseException e) {
                e.printStackTrace();
            }
        }
    }

    // 获取指定日期的当天开始时间
    private static Date getStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    // 获取指定日期的当天结束时间
    private static Date getEndOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        return calendar.getTime();
    }

    // 格式化日期
    private static String formatDate(Date date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    public static String getIndexName(String time) {
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = null;
        try {
            date = format.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        if (date != null) {
            calendar.setTime(date);
        } else {
            return null;
        }
        // 获得当前日期是一个星期的第几天
        int dayWeek = calendar.get(Calendar.DAY_OF_WEEK);
        if (dayWeek == 1) {
            dayWeek = 8;
        }
        // 根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
        calendar.add(Calendar.DATE, calendar.getFirstDayOfWeek() - dayWeek);
        DateFormat newFormat = new SimpleDateFormat("yyyy.MM.dd");
        return "motorvehicle-" + newFormat.format(calendar.getTime());

    }
}
