package com.yang.elasticsearch.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticsearchTest {

    public static void main(String[] args) throws Exception {
    
        // 创建一个 RestHighLevelClient 实例，连接到本地主机的默认端口（9200）
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
            
        try {
            // 使用 client 执行一些基本的 Elasticsearch 请求，例如：
            ClusterHealthResponse response = client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
            System.out.println(response.getStatus());
        } catch (Exception e) {
            // 处理异常
            e.printStackTrace();
        } finally {
            // 关闭 client
            client.close();
        }
    }
}
