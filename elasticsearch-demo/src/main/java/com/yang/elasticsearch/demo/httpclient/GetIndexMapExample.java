package com.yang.elasticsearch.demo.httpclient;

import cn.hutool.json.JSONUtil;
import cn.hutool.json.JSONObject;
import com.yang.elasticsearch.demo.highlevel.Commons;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @author admin
 * @desc:单机集群6.5.4和shiny集群6.1.3测试通过
 */

public class GetIndexMapExample {

        private static String indexName = "my_index2";
//    private static String indexName = "motorvehicle-2023.01.02";

    public static void main(String[] args) throws IOException {

        CloseableHttpClient httpClient = HttpClients.createDefault();
        try{

            HttpGet httpGet = Commons.getShinyHttpClient(indexName);
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity httpEntity = response.getEntity();
            String result = httpEntity != null ? EntityUtils.toString(httpEntity) :null;
            JSONObject jsonObject = JSONUtil.parseObj(result);

            JSONObject fields = jsonObject.getJSONObject(indexName).getJSONObject("mappings").getJSONObject("doc").getJSONObject("properties");

            System.out.println(fields);

        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            httpClient.close();
        }
    }
}
