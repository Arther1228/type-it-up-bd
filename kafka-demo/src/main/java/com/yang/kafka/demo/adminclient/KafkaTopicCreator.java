package com.yang.kafka.demo.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author admin
 * @desc:单机集群2.11-2.1.0测试通过
 * //TODO shiny集群2.11-0.11.0.1未测试通过
 */
public class KafkaTopicCreator {

    private static String local_cluster_server =  "http://localhost:9092";
    private static String shiny_cluster_server =  "http://34.8.8.115:21005,http://34.8.8.109:21005,http://34.8.8.116:21005";

    private static int numPartitions = 3;
    private static short replicationFactor = 1;

    public static void createTopic(String bootstrapServers, String topicName, int numPartitions, int replicationFactor) {

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
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
            // Add the topic to the list of topics to be created
            CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
            System.out.println(topics.values().size());
        }
    }

    /**
     * @param bootstrapServers
     * @param topicName
     * @throws ExecutionException
     * @throws InterruptedException((HashMap) topics.futures).size
     * @desc //TODO 代码有问题
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

    public static void main(String[] args) throws Exception {

        String topicName = "clicks4";
        createTopic(local_cluster_server, topicName, numPartitions, replicationFactor);

    }
}
