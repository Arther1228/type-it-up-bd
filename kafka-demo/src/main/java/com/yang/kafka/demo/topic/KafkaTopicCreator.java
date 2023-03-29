package com.yang.kafka.demo.topic;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author admin
 */
public class KafkaTopicCreator {

    public static void createTopic(String bootstrapServers, String topicName) throws Exception {

        // Set up the Kafka admin client properties
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("acks", "all");
        props.put(AdminClientConfig.RETRIES_CONFIG, 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        // Create a Kafka admin client
        try (AdminClient adminClient = AdminClient.create(props)) {
            // Create the topic config
//            Map<String, String> config = Collections.singletonMap("message.format", "json");

            // Create the topic with the specified config and partitions/replication factor
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

            // Add the topic to the list of topics to be created
            CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
            System.out.println(topics.values().size());
        }
    }

    /**
     * @desc  代码有问题
     * @param bootstrapServers
     * @param topicName
     * @throws ExecutionException
     * @throws InterruptedException((HashMap) topics.futures).size
     */
    public static void deleteTopic(String bootstrapServers, String topicName) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
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

    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String topicName = "clicks3";

        // Create the Kafka topic
        createTopic(bootstrapServers, topicName);

//        deleteTopic(bootstrapServers, topicName);

    }
}
