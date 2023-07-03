package com.yang.elasticsearch.demo.httpclient;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @desc：对应雪亮集群这种索引很多的情况，http请求响应时间较长，可能不使用此方法
 */
public class ElasticsearchIndexList {
    public static void main(String[] args) {
        String elasticsearchUrl = "http://localhost:9200/_cat/indices?format=json";

        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(elasticsearchUrl);

        try {
            HttpResponse response = httpClient.execute(request);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                String jsonString = EntityUtils.toString(entity);
                List<String> indexList = parseIndexList(jsonString);
                
                // 打印索引列表
                for (String index : indexList) {
                    System.out.println(index);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<String> parseIndexList(String jsonString) {
        List<String> indexList = new ArrayList<>();

        Gson gson = new Gson();
        JsonArray jsonArray = gson.fromJson(jsonString, JsonArray.class);
        
        for (JsonElement jsonElement : jsonArray) {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            String indexName = jsonObject.get("index").getAsString();
            indexList.add(indexName);
        }
        return indexList;
    }
}
