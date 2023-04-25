package com.yang.elasticsearch.demo.highlevel;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author admin
 * @desc:单机集群6.5.4测试通过
 * //TODO shiny集群6.1.3未测试通过
 */

public class GetIndexMapExample {

    //    private static String indexName = "user-info";
    private static String indexName = "motorvehicle-2023.01.02";

    public static void main(String[] args) {

        try {
//        RestHighLevelClient client = Commons.getLocalClusterClient();
            RestHighLevelClient client = Commons.getShinyClusterClient();
            GetMappingsRequest request = new GetMappingsRequest().indices(indexName);
            GetMappingsResponse getResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = getResponse.mappings();
            ImmutableOpenMap<String, MappingMetaData> mappingData = mappings.get(indexName);
            Map<String, Object> sourceMap = mappingData.get("doc").getSourceAsMap();
            LinkedHashMap<String, Object> properties = (LinkedHashMap<String, Object>) sourceMap.get("properties");
            properties.forEach((key, value) -> {
                        System.out.println(key);
                        LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) value;
                        System.out.println(map.get("type"));
                    }
            );

            client.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
