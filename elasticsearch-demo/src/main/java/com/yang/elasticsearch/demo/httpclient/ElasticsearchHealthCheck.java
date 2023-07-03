package com.yang.elasticsearch.demo.httpclient;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class ElasticsearchHealthCheck {
    public static void main(String[] args) {
        // Elasticsearch集群的主机和端口
        String host = "localhost";
        int port = 9200;
        // 构建HTTP请求的URL
        String url = "http://" + host + ":" + port + "/_cluster/health";
        // 创建HttpClient实例
        CloseableHttpClient httpClient = HttpClients.createDefault();
        // 创建HttpGet请求对象
        HttpGet httpGet = new HttpGet(url);
        try {
            // 执行请求并获取响应
            CloseableHttpResponse response = httpClient.execute(httpGet);
            // 从响应中获取响应体
            String responseBody = EntityUtils.toString(response.getEntity());
            JSONObject jsonObject = JSONUtil.parseObj(responseBody);
            String status = (String) jsonObject.get("status");
            // 输出响应体
            System.out.println(status);

            // 关闭响应
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭HttpClient
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
