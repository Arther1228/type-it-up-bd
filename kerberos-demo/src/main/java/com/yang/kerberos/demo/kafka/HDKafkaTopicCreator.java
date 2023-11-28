package com.yang.kerberos.demo.kafka;

import com.yang.kerberos.demo.util.KerberosUtil;
import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author admin
 * @desc:shiny集群2.11-0.11.0.1未测试通过,原因有两个：
 * （1）创建topic是需要认证的；可能是配置出了问题；（虽然配置了 allow.everyone.if.no.acl.found = true 和 auto.create.topics.enable=true）
 * （2）之前的washout没有足够的权限；
 * （3）kafka-client的包需要使用华为的包！！！！！！！
 */
public class HDKafkaTopicCreator {

    private static int numPartitions = 3;
    private static short replicationFactor = 1;

    public static void main(String[] args) {
        String topicName = "clicks1";
        AdminClient adminClient = KerberosUtil.createKafkaClient();
        try {
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
