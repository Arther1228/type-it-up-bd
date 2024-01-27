package com.yang.elasticsearch.demo.aggregation;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.support.ExcelTypeEnum;
import com.alibaba.excel.write.metadata.WriteSheet;
import lombok.Data;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchDataExporterExcel {

    private String esHost = "34.8.8.93";

    private int esPort = 24102;

    private static String excelFilePath = "output.xlsx";

    private String indexName = "shiny-pt-oper-log";

    private List<ExportData> searchEsData() {

        List<ExportData> result = new ArrayList<>();

        String field1 = "oper_user_name";
        String field2 = "oper_real_name";
        String field3 = "org_level_two_name";

        // 连接 Elasticsearch
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(esHost, esPort, "http")))) {
            // 创建查询请求
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);

            // 创建布尔查询
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            boolQuery.must(QueryBuilders.rangeQuery("oper_time")
                    .gte(20230127000000L)
                    .lte(20240127000000L));

            boolQuery.must(QueryBuilders.termsQuery("oper_mod_id", new long[]{21020010}));

            // 将布尔查询设置为主查询条件
            searchSourceBuilder.query(boolQuery);

            // 设置聚合条件
            TermsAggregationBuilder agg1 = AggregationBuilders.terms("user_name_agg")
                    .field(field1)
                    .size(10000);
            TermsAggregationBuilder agg2 = AggregationBuilders.terms("real_name_agg")
                    .field(field2)
                    .size(10000);
            TermsAggregationBuilder agg3 = AggregationBuilders.terms("org_level_two_agg")
                    .field(field3)
                    .size(10000);

            agg1.subAggregation(agg2.subAggregation(agg3));
            searchSourceBuilder.aggregation(agg1);

            // 执行查询
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            result.addAll(resolveData(searchResponse));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }


    private List<ExportData> resolveData(SearchResponse searchResponse) {
        List<ExportData> result = new ArrayList<>();

        ParsedStringTerms agg1 = searchResponse.getAggregations().get("user_name_agg");
        for (Terms.Bucket agg1Bucket : agg1.getBuckets()) {
            String realName = "";
            String org_name_two = "";
            long count = 0;

            String field1Value = agg1Bucket.getKeyAsString();
            String userName = field1Value;

            ParsedStringTerms agg2 = agg1Bucket.getAggregations().get("real_name_agg");
            int number1 = 0;
            for (Terms.Bucket agg2Bucket : agg2.getBuckets()) {
                number1++;
                String field2Value = agg2Bucket.getKeyAsString();
                if (number1 > 1) {
                    realName += "/" + field2Value;
                } else {
                    realName += field2Value;
                }

                ParsedStringTerms agg3 = agg2Bucket.getAggregations().get("org_level_two_agg");
                int number2 = 0;
                for (Terms.Bucket agg3Bucket : agg3.getBuckets()) {
                    number2++;
                    String field3Value = agg3Bucket.getKeyAsString();
                    if ("无".equals(field3Value)) {
                        field3Value = "市局";
                    }
                    if (number2 > 1) {
                        org_name_two += "/" + field3Value;
                    } else {
                        org_name_two += field3Value;
                    }
                    count += agg3Bucket.getDocCount();
                }
            }
            ExportData exportData = new ExportData(userName, realName, org_name_two, count);
            result.add(exportData);
        }
        return result;
    }

    private static void processAndWriteToExcel(List<ExportData> result, String outputFilePath) {

        // 使用 EasyExcel 写入 Excel
        ExcelWriter excelWriter = EasyExcel.write(outputFilePath, ExportData.class).excelType(ExcelTypeEnum.XLSX).build();
        try {
            WriteSheet writeSheet = EasyExcel.writerSheet(1, "Sheet1").build();
            excelWriter.write(result, writeSheet);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error: " + e.getMessage());
        } finally {
            // 关闭 ExcelWriter
            if (excelWriter != null) {
                excelWriter.finish();
            }
        }
    }

    // 定义一个用于写入 Excel 的数据类
    @Data
    static class ExportData {

        private String oper_user_name;
        private String oper_real_name;
        private String org_level_two_name;
        private long count;

        public ExportData(String oper_user_name, String oper_real_name, String org_level_two_name, long count) {
            this.oper_user_name = oper_user_name;
            this.oper_real_name = oper_real_name;
            this.org_level_two_name = org_level_two_name;
            this.count = count;
        }
    }

    public static void main(String[] args) {

        ElasticsearchDataExporterExcel elasticsearchDataExporterExcel = new ElasticsearchDataExporterExcel();

        List<ExportData> result = new ArrayList<>();
        result.add(new ExportData("1", "2", "3", 1));

        // 处理查询结果并写入 Excel
        ElasticsearchDataExporterExcel.processAndWriteToExcel(result, excelFilePath);

    }
}
