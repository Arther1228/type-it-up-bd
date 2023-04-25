package com.yang.elasticsearch.demo.highlevel;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 存储数据
 * @desc:shiny集群6.1.3和单机集群6.5.4测试通过
 */
public class ElasticSearchIndexingExample {

    private static String indexName = "my_index2";

    public static void main(String[] args) throws IOException {

        // Create a RestHighLevelClient instance
//        RestHighLevelClient client = Commons.getLocalClusterClient();
        RestHighLevelClient client = Commons.getShinyClusterClient();

        // Define the document to be indexed
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "james");
        jsonMap.put("age", 123);
        jsonMap.put("email", "example@email.com");

        // Define the index request and set the source document
        IndexRequest indexRequest = new IndexRequest(indexName).type("doc")
                .source(jsonMap, XContentType.JSON);

        // Execute the index request and get the response
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

        // Print the response details
        System.out.println("Indexed document id: " + indexResponse.getId());
        System.out.println("Index: " + indexResponse.getIndex());
        System.out.println("Type: " + indexResponse.getType());
        System.out.println("Version: " + indexResponse.getVersion());
        
        // Close the client connection
        client.close();
    }
}
