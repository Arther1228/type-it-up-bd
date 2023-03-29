package com.yang.kafka.demo.adminclient;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;

/**
 * @author admin
 */
public class KafkaClusterChecker {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092"); //填写kafka broker地址和端口号
        AdminClient adminClient = AdminClient.create(props);
        DescribeClusterResult describeCluster = adminClient.describeCluster();
        try {
            String clusterId = describeCluster.clusterId().get();
            System.out.println("Kafka cluster is up and running.");
        } catch (Exception e) {
            System.out.println("Error while checking kafka cluster:" + e.getMessage());
        } finally {
            adminClient.close();
        }
    }
}
