package com.yang.elasticsearch.demo.restapi;

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
import java.util.HashMap;

/**
 * @author admin
 * @desc:单机集群6.5.4和shiny集群6.1.3测试通过
 */

public class GetIndexMapExample {

    private static String indexName = "test4";
//    private static String indexName = "motorvehicle-2023.01.02";

    public static void main(String[] args) throws IOException {

        CloseableHttpClient httpClient = HttpClients.createDefault();
        try{

//            HttpGet httpGet = Commons.getShinyHttpClient(indexName);
            HttpGet httpGet = Commons.getLocalHttpClient(indexName);
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity httpEntity = response.getEntity();
            String result = httpEntity != null ? EntityUtils.toString(httpEntity) :null;
            JSONObject jsonObject = JSONUtil.parseObj(result);

            JSONObject fields = jsonObject.getJSONObject(indexName).getJSONObject("mappings").getJSONObject("doc").getJSONObject("properties");
            HashMap<String, String> columnsMap = new HashMap<>(2);
            fields.forEach((key, value) -> {
                        JSONObject jsonValue  = (JSONObject)value;
                        String type =  (String)jsonValue.get("type");
                        if ("date".equals(type)) {
                            String format =  (String)jsonValue.get("format");
                            columnsMap.put(key, type + " | " + (format == null ? "" : format));
                        } else {
                            columnsMap.put(key, type);
                        }
                    }
            );
            System.out.println(columnsMap);

        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            httpClient.close();
        }
    }
}
