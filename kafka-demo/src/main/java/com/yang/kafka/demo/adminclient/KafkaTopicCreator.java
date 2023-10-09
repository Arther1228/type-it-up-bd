package com.yang.kafka.demo.adminclient;

import com.yang.kafka.demo.Commons;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author admin
 * @desc:单机集群2.11-2.1.0测试通过
 * @desc:shiny集群2.11-0.11.0.1未测试通过,原因有两个：
 * （1）创建topic是需要认证的；可能是配置出了问题；（虽然配置了 allow.everyone.if.no.acl.found = true 和 auto.create.topics.enable=true）
 * （2）之前的washout没有足够的权限；
 * （3）kafka-client的包需要使用华为的包！！！！！！！
 */
public class KafkaTopicCreator {

    private static int numPartitions = 3;
    private static short replicationFactor = 1;

    public static void main(String[] args) {
        String topicName = "clicks2";
        String bootstrapServers = Commons.getShinyClusterServer();
        Properties props = Commons.initProperties(bootstrapServers);
        try {
            AdminClient adminClient = AdminClient.create(props);
            // Create a new topic with one partition and a replication factor of one
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
//            newTopic.configs(Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG, "86400000"));
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic), new CreateTopicsOptions().timeoutMs(2000));
            // Check if the topic was successfully created
            createTopicsResult.values().get(topicName).get(5, TimeUnit.SECONDS);
            System.out.println("Topic created successfully");
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("Failed to create topic: " + e.getMessage());
        }
    }
}
