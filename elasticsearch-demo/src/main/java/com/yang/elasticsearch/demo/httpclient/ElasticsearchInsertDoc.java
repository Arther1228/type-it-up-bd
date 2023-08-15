package com.yang.elasticsearch.demo.httpclient;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class ElasticsearchInsertDoc {
    public static void main(String[] args) {
        // Elasticsearch集群的主机和端口
        String host = "localhost";
        int port = 9200;
        // 构建HTTP请求的URL
        String url = "http://" + host + ":" + port + "/my_index/doc";
        // 创建HttpClient实例
        CloseableHttpClient httpClient = HttpClients.createDefault();
        // 创建HttpGet请求对象
        HttpPost httpGet = new HttpPost(url);
        String jsonPayload = "{\"age\":1, \"email\":\"123@qq.com\", \"name\":\"zhansan\"}";
        httpGet.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));
        try {
            // 执行请求并获取响应
            CloseableHttpResponse response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 201) {
                System.out.println("文档新增成功");
            } else {
                System.err.println("文档新增失败，HTTP状态码：" + statusCode);
            }
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
