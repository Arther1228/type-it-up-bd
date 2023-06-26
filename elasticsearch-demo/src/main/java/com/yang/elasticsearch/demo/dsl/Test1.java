package com.yang.elasticsearch.demo.dsl;

import com.kanlon.utils.SQLToEsDSLUtils;
import com.yang.elasticsearch.demo.highlevel.Commons;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author yangliangchuang 2023/6/26 8:58
 */
public class Test1 {

    private static String indexName = "access_authority";

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = Commons.getLocalClusterClient();
        SearchRequest searchRequest = new SearchRequest(indexName);

        String sql = "select * from access_authority where action = 'query'";
        SearchSourceBuilder searchSourceBuilder = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 处理搜索结果
        System.out.println(searchResponse.toString());

    }

}
