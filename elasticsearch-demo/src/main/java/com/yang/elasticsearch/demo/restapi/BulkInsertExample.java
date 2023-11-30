package com.yang.elasticsearch.demo.restapi;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * 1、postman 测试bulk命令时，需要在json最后加一个空行
 * 2、postman 提示当前json不是标准的json
 */
public class BulkInsertExample {

    public static void main(String[] args) {
        CloseableHttpClient httpClient = HttpClients.createDefault();

        try {
            HttpPost httpPost = new HttpPost("http://localhost:9200/_bulk");
            httpPost.addHeader("Content-Type", "application/json");

            String bulkRequestBody =
                "{ \"index\" : { \"_index\" : \"your_index\", \"_type\" : \"your_type\", \"_id\" : \"1\" } }\n" +
                "{ \"field1\" : \"value1\" }\n" +
                "{ \"index\" : { \"_index\" : \"your_index\", \"_type\" : \"your_type\", \"_id\" : \"2\" } }\n" +
                "{ \"field2\" : \"value2\" }\n";

            HttpEntity entity = new StringEntity(bulkRequestBody);
            httpPost.setEntity(entity);

            CloseableHttpResponse response = httpClient.execute(httpPost);

            try {
                StatusLine statusLine = response.getStatusLine();
                int statusCode = statusLine.getStatusCode();

                if (statusCode == 200 || statusCode == 201) {
                    System.out.println("Bulk insert successful");
                } else {
                    System.out.println("Bulk insert failed, HTTP status code: " + statusCode);
                }

                // Optionally, you can also read and log the response entity
                // String responseString = EntityUtils.toString(response.getEntity());
                // System.out.println("Response from Elasticsearch: " + responseString);
            } finally {
                response.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
