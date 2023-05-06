package com.yang.kerberos.demo.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author admin
 * @desc:shiny集群2.11-0.11.0.1未测试通过,原因有两个：
 * （1）创建topic是需要认证的；可能是配置出了问题；（虽然配置了 allow.everyone.if.no.acl.found = true 和 auto.create.topics.enable=true）
 * （2）之前的washout没有足够的权限；
 * （3）kafka-client的包需要使用华为的包！！！！！！！
 */
public class KafkaKerberosApi {

    private static int numPartitions = 3;
    private static short replicationFactor = 1;

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "D:\\ylc\\code\\type-it-up-bd\\kerberos-demo\\config\\krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("java.security.auth.login.config", "D:\\ylc\\code\\type-it-up-bd\\kerberos-demo\\config\\jaas.conf");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.8.8.115:21007");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-connection");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");

        String topicName = "clicks2";
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