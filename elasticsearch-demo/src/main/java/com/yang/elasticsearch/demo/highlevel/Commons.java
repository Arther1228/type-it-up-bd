package com.yang.elasticsearch.demo.highlevel;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Arrays;

/**
 * @author yangliangchuang 2023/4/24 16:44
 */
public class Commons {

    private static  String local_cluster_url = "http://34.8.99.80:9200";

    private static String shiny_cluster_url = "http://34.8.8.122:24100,http://34.8.8.123:24100,http://34.8.8.124:24100," +
            "http://34.8.8.125:24100,http://34.8.8.126:24100,http://34.8.8.127:24100";

    public static RestHighLevelClient getLocalClusterClient(){
        return initHighLevelClient(local_cluster_url);
    }

    public static RestHighLevelClient getShinyClusterClient(){
        return initHighLevelClient(shiny_cluster_url);
    }

    public static RestHighLevelClient initHighLevelClient(String nodeUrl){

        HttpHost[] httpHosts = Arrays.stream(nodeUrl.split(",")).map(url -> {
            String ipPortStr = url.substring(url.indexOf("//") + 2);
            String[] ipPort = ipPortStr.split(":");
            return new HttpHost(ipPort[0], Integer.valueOf(ipPort[1]));
        }).toArray(HttpHost[]::new);

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHosts));
        return client;

    }

    public static HttpGet getShinyHttpClient(String indexName){
        return initHttpGet(shiny_cluster_url, indexName);
    }

    public static HttpGet initHttpGet(String nodeUrl, String indexName){

        String[] urls = nodeUrl.split(",");
        String getUrl = urls[0] + "/" + indexName + "/_mapping";
        HttpGet httpGet = new HttpGet(getUrl);
        return httpGet;

    }

}
