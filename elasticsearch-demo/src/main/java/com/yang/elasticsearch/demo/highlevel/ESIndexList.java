package com.yang.elasticsearch.demo.highlevel;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @desc:shiny集群6.1.3和单机集群6.5.4测试通过
 */
public class ESIndexList {

    public static void main(String[] args) throws IOException {
//        RestHighLevelClient client = EsClientUtil.getLocalClusterClient();
        RestHighLevelClient client = Commons.getShinyClusterClient();

        try {
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesResponse getAliasesResponse = client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> map = getAliasesResponse.getAliases();
            Set<String> indices = map.keySet();
            for (String key : indices) {
                System.out.println(key);
            }
            List<String> collect = indices.stream().filter(s -> !s.startsWith("."))
                    .collect(Collectors.toList());
            System.out.println(collect);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // 关闭客户端连接
        client.close();
    }
}
