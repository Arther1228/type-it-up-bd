package com.yang.elasticsearch.demo;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;

/**
 * @author admin
 */
public class GetIndexMapExample {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

        GetMappingsRequest getRequest = new GetMappingsRequest().indices("user-info");

        GetMappingsResponse getResponse = client.indices().getMapping(getRequest, RequestOptions.DEFAULT);

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getResponse.mappings();

        ImmutableOpenMap<String, MappingMetaData> mappingData = mappings.get("user-info");
        Map<String, Object> sourceMap = mappingData.get("doc").getSourceAsMap();

        LinkedHashMap<String, Object> properties = (LinkedHashMap<String, Object>) sourceMap.get("properties");

        properties.forEach((key, value) -> {
                    System.out.println(key);
                    LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) value;
                    System.out.println(map.get("type"));
                }
        );

        client.close();
    }
}
