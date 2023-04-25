package com.yang.elasticsearch.demo.highlevel;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @desc:shiny集群6.1.3和单机集群6.5.4测试通过
 */
public class ElasticsearchTest {

    public static void main(String[] args) throws Exception {

        RestHighLevelClient client =  Commons.getLocalClusterClient();
//        RestHighLevelClient client = Commons.getShinyClusterClient();

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
