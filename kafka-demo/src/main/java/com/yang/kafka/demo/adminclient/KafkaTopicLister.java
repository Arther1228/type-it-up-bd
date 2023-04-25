package com.yang.kafka.demo.adminclient;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

/**
 * @desc:shiny集群2.11-0.11.0.1和单机集群2.11-2.1.0测试通过
 */
public class KafkaTopicLister {

    private static String local_cluster_server =  "http://localhost:9092";
    private static String shiny_cluster_server =  "http://34.8.8.115:21005,http://34.8.8.109:21005,http://34.8.8.116:21005";

    public static void main(String[] args) {
        Properties props = new Properties();
//        props.put("bootstrap.servers", local_cluster_server);
        props.put("bootstrap.servers", shiny_cluster_server);
        try (AdminClient client = AdminClient.create(props)) {
            ListTopicsResult topics = client.listTopics();
            topics.names().get().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
