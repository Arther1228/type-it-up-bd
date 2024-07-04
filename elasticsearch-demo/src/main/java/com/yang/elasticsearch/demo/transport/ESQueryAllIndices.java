package com.yang.elasticsearch.demo.transport;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * tanrsport client 版本 至少 6.8.0 才可以访问 7.17.6
 *
 */
public class ESQueryAllIndices {

    public static void main(String[] args) {
        try {
            // 设置客户端配置
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch") // 替换为你的集群名称
                    .build();

            // 创建TransportClient
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.99.5"), 9300));

            // 构建获取索引请求
            GetIndexRequest request = new GetIndexRequest();
            request.indices("*");

            // 执行请求并获取响应
            GetIndexResponse response = client.admin().indices().getIndex(request).actionGet();

            // 打印所有索引
            System.out.println("All indices:");
            for (String index : response.getIndices()) {
                System.out.println(index);
            }

            // 关闭客户端
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
