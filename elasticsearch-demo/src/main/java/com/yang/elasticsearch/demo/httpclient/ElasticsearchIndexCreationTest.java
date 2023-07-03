package com.yang.elasticsearch.demo.httpclient;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ElasticsearchIndexCreationTest {
    public static void main(String[] args) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpHost host = new HttpHost("localhost", 9200, "http"); // Elasticsearch主机和端口

        String indexName = "test7"; // 要创建的索引名称
        String jsonPayload = "{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 1,\n" +
                "    \"number_of_replicas\": 1\n" +
                "  },\n" +
                "  \"mappings\": {\n" +
                "  \"doc\" : {\n" +
                "    \"properties\": {\n" +
                "      \"title\": { \"type\": \"text\" },\n" +
                "      \"description\": { \"type\": \"text\" }\n" +
                "    }\n" +
                "  }\n" +
                "  }\n" +
                "}\n"; // 索引的映射和设置

        String endpoint = "/" + indexName; // Elasticsearch索引的API端点

        try {
            HttpPut request = new HttpPut(endpoint);
            request.addHeader("Content-Type", "application/json");
            request.setEntity(new StringEntity(jsonPayload));

            CloseableHttpResponse response = httpClient.execute(host, request);
            HttpEntity entity = response.getEntity();

            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
            String line;
            StringBuilder responseBuilder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                responseBuilder.append(line);
            }

            System.out.println("Response: " + responseBuilder.toString());

            response.close();
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
