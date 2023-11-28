package com.yang.kafka.demo.adminclient;

import java.util.Properties;

import com.yang.kafka.demo.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.junit.Test;

/**
 * @author admin
 * @desc:shiny集群2.11-0.11.0.1和单机集群2.11-2.1.0测试通过
 */
@Slf4j
public class KafkaClusterChecker {

    @Test
    public void check() {
        Properties props = new Properties();
//        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtil.getLocalClusterServer());
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaUtil.getShinyClusterServer());
        AdminClient adminClient = AdminClient.create(props);
        DescribeClusterResult describeCluster = adminClient.describeCluster();
        try {
            String clusterId = describeCluster.clusterId().get();
            log.info("Kafka cluster is up and running. clusterId:{}", clusterId);
        } catch (Exception e) {
            System.out.println("Error while checking kafka cluster:" + e.getMessage());
        } finally {
            adminClient.close();
        }
    }
}
