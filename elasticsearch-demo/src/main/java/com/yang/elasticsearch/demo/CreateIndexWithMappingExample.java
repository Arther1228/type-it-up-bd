package com.yang.elasticsearch.demo;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author admin
 */
public class CreateIndexWithMappingExample {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", new HashMap<String, Object>() {{
            put("name", new HashMap<String, Object>() {{
                put("type", "text");
            }});
            put("age", new HashMap<String, Object>() {{
                put("type", "integer");
            }});
            put("email", new HashMap<String, Object>() {{
                put("type", "keyword");
            }});
        }});

        String mappingString = JSON.toJSONString(mapping);

        CreateIndexRequest request = new CreateIndexRequest("my_index");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        request.mapping("doc", mappingString, XContentType.JSON);

        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println("Index created: " + response.isAcknowledged());

        client.close();
    }
}
