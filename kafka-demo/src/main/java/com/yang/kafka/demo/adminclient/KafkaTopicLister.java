package com.yang.kafka.demo.adminclient;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class KafkaTopicLister {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // replace with your kafka bootstrap servers
        try (AdminClient client = AdminClient.create(props)) {
            ListTopicsResult topics = client.listTopics();
            topics.names().get().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
