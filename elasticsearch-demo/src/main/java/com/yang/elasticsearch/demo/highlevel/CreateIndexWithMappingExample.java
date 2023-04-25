package com.yang.elasticsearch.demo.highlevel;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @desc:shiny集群6.1.3和单机集群6.5.4测试通过
 */
public class CreateIndexWithMappingExample {

    private static String indexName = "my_index2";
    private static int number_of_shards = 3;
    private static int number_of_replicas = 1;

    public static void main(String[] args) throws IOException {

//        RestHighLevelClient client = Commons.getLocalClusterClient();
        RestHighLevelClient client = Commons.getShinyClusterClient();

        Map<String, Object> mapping = new HashMap<>(3);
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
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", number_of_shards)
                .put("index.number_of_replicas", number_of_replicas)
        );
        request.mapping("doc", mappingString, XContentType.JSON);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("Index created: " + response.isAcknowledged());

        client.close();
    }
}
