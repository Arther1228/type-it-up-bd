package com.yang.kafka.demo.adminclient;

import java.util.Properties;

import com.yang.kafka.demo.Commons;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;

/**
 * @author admin
 * @desc:shiny集群2.11-0.11.0.1和单机集群2.11-2.1.0测试通过
 */
public class KafkaClusterChecker {

    public static void main(String[] args) {
        Properties props = new Properties();
//        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.getLocal_cluster_server());
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.getShiny_cluster_server());
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
