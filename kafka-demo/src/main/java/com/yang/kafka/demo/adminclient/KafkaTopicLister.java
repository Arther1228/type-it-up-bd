package com.yang.kafka.demo.adminclient;

import java.util.Properties;

import com.yang.kafka.demo.util.KafkaUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

/**
 * @desc:
 * 单机集群2.11-2.1.0测试通过
 * shiny集群2.11-0.11.0.1测试通过
 */
public class KafkaTopicLister {


    public static void main(String[] args) {
        Properties props = new Properties();
        //      props.put("bootstrap.servers", Commons.getLocalClusterServer());
        props.put("bootstrap.servers", KafkaUtil.getShinyClusterServer());
        try (AdminClient client = AdminClient.create(props)) {
            ListTopicsResult topics = client.listTopics();
            topics.names().get().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
