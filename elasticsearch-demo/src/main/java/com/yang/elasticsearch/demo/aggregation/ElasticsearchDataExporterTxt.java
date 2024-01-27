package com.yang.elasticsearch.demo.aggregation;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.FileWriter;
import java.io.IOException;

public class ElasticsearchDataExporterTxt {

    private static String field1 = "oper_user_name";

    public static void main(String[] args) {
        // 设置 Elasticsearch 服务器地址
        String esHost = "34.8.8.93";
        int esPort = 24102;

        // 设置 Excel 文件路径
        String excelFilePath = "output.txt";

        // 设置索引名称和字段
        String indexName = "shiny-pt-sso-log";

        // 连接 Elasticsearch
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(esHost, esPort, "http"))
        )) {
            // 创建查询请求
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);

            // 创建布尔查询
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            boolQuery.must(QueryBuilders.rangeQuery("oper_time")
                    .gte(20230127000000L)
                    .lte(20240127000000L));

            // 将布尔查询设置为主查询条件
            searchSourceBuilder.query(boolQuery);

            // 设置聚合条件
            TermsAggregationBuilder agg1 = AggregationBuilders.terms("user_name_agg")
                    .field(field1)
                    .size(10000);

            searchSourceBuilder.aggregation(agg1);

            // 执行查询
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            // 处理查询结果并写入 Excel
            processAndWriteToTxt(searchResponse, excelFilePath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processAndWriteToTxt(SearchResponse searchResponse, String outputFilePath) {
        try (FileWriter writer = new FileWriter(outputFilePath)) {
            ParsedStringTerms agg1 = searchResponse.getAggregations().get("user_name_agg");

            for (Terms.Bucket agg1Bucket : agg1.getBuckets()) {
                String field1Value = agg1Bucket.getKeyAsString();
                long count = agg1Bucket.getDocCount();
                writer.write(field1Value + ",");
                writer.write(count + "\n");
            }

            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
