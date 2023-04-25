package com.yang.kafka.demo.adminclient;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;

/**
 * @author admin
 * @desc:shiny集群2.11-0.11.0.1和单机集群2.11-2.1.0测试通过
 */
public class KafkaClusterChecker {

    private static String local_cluster_server =  "http://localhost:9092";
    private static String shiny_cluster_server =  "http://34.8.8.115:21005,http://34.8.8.109:21005,http://34.8.8.116:21005";

    public static void main(String[] args) {
        Properties props = new Properties();
//        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, local_cluster_server);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, shiny_cluster_server);
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
