package com.yang.kafka.demo.offset.demo4;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author JMWANG
 */
@Component
public class OffsetApplicationProperties implements InitializingBean {

    @Value("${kafka.consumer.servers}")
    private String serverHostPort;

    @Value("${jmw.kafka.offset.zookeeper.connect}")
    private String zookeeperConnectHostPort;

    @Value("${kafka.consumer.topic}")
    private String offsetTopic;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.enable.auto.commit}")
    private String autoAutoCommit;

    @Value("${offset.key.deserializer}")
    private String key;

    @Value("${offset.value.deserializer}")
    private String value;

    /**
     * kafka server url
     */
    public static String KAFKA_OFFSET_SERVER_HOST_PORT;

    /**
     * kafka server url
     */
    public static String KAFKA_OFFSET_ZOOKEEPER_CONNECT;

    /**
     * kafka offset topic
     */
    public static String KAFKA_OFFSET_TOPIC;

    public static String KAFKA_OFFSET_GROUP_ID;

    public static String KAFKA_OFFSET_ENABLE_AUTO_COMMIT;

    public static String KAFKA_KEY_SERIALIZER;

    public static String KAFKA_VALUE_SERIALIZER;

    @Override
    public void afterPropertiesSet() throws Exception {
        KAFKA_OFFSET_SERVER_HOST_PORT = serverHostPort;
        KAFKA_OFFSET_ZOOKEEPER_CONNECT = zookeeperConnectHostPort;
        KAFKA_OFFSET_TOPIC = offsetTopic;
        KAFKA_OFFSET_GROUP_ID = groupId;
        KAFKA_OFFSET_ENABLE_AUTO_COMMIT = autoAutoCommit;
        KAFKA_KEY_SERIALIZER = key;
        KAFKA_VALUE_SERIALIZER = value;
    }
}

