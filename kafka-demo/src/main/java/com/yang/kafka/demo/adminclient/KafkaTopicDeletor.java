package com.yang.kafka.demo.adminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author admin
 * //TODO 代码有问题，windows删除会把kafka集群搞挂
 */
public class KafkaTopicDeletor {

    /**
     * @param bootstrapServers
     * @param topicName
     * @throws ExecutionException
     * @throws InterruptedException((HashMap) topics.futures).size
     */
    public static void deleteTopic(String bootstrapServers, String topicName) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (AdminClient adminClient = AdminClient.create(props)) {
            DeleteTopicsOptions options = new DeleteTopicsOptions();
            options.timeoutMs(5000);

            // Check if the topic exists before trying to delete it
            boolean topicExists = false;
            try {
                adminClient.describeTopics(Collections.singleton(topicName)).all().get();
                topicExists = true;
            } catch (Exception e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    System.out.println("Topic " + topicName + " does not exist.");
                } else {
                    System.out.println("describe topic error.");
                }
            }
            if (topicExists) {
                adminClient.deleteTopics(Collections.singleton(topicName), options).all().get();
                System.out.println("Topic " + topicName + " deleted successfully.");
            }
        }
    }

}
