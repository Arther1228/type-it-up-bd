package com.yang.elasticsearch.demo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.RequestOptions;

public class ElasticSearchIndexingExample {

    public static void main(String[] args) throws IOException {

        // Create a RestHighLevelClient instance
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

        // Define the document to be indexed
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user_create_time", "20210322063701");
        jsonMap.put("user_email", "example@email.com");
        jsonMap.put("user_id", 123);
        jsonMap.put("user_name", "example_username");
        jsonMap.put("user_pwd", "p@ssword");
        jsonMap.put("user_real_name", "Example User");
        jsonMap.put("user_status", "active");
        jsonMap.put("user_tel", "+1234567890");
        jsonMap.put("user_update_time", "20210322063701");

        // Define the index request and set the source document
        IndexRequest indexRequest = new IndexRequest("user-info").type("doc")
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
