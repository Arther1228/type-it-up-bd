package com.yang.kerberos.demo.security;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author yangliangchuang 2023/5/6 15:07
 */
public class KafkaKerberosDemo {

    private final static Properties props = new Properties();

    // Broker地址列表
    private final static String bootstrapServers = "bootstrap.servers";

    // 客户端ID
    private final static String clientId = "client.id";

    // Key序列化类
    private final static String keySerializer = "key.serializer";

    // Value序列化类
    private final static String valueSerializer = "value.serializer";

    // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
    private final static String securityProtocol = "security.protocol";

    // 服务名
    private final static String saslKerberosServiceName = "sasl.kerberos.service.name";

    // 域名
    private final static String kerberosDomainName = "kerberos.domain.name";

    static {
        KafkaProperties kafkaProc = KafkaProperties.getInstance();
        // Broker地址列表
        props.put(bootstrapServers, kafkaProc.getValues(bootstrapServers, "34.8.8.115:21007"));
        // 客户端ID
        props.put(clientId, kafkaProc.getValues(clientId, "DemoProducer"));
        // Key序列化类
        props.put(keySerializer, kafkaProc.getValues(keySerializer, "org.apache.kafka.common.serialization.IntegerSerializer"));
        // Value序列化类
        props.put(valueSerializer, kafkaProc.getValues(valueSerializer, "org.apache.kafka.common.serialization.StringSerializer"));
        // 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
        props.put(securityProtocol, kafkaProc.getValues(securityProtocol, "SASL_PLAINTEXT"));
        // 服务名
        props.put(saslKerberosServiceName, "kafka");
        // 域名
        props.put(kerberosDomainName, kafkaProc.getValues(kerberosDomainName, "hadoop.hadoop.com"));
    }

    public static void main(String[] args) {
        //安全模式设置
        SecurityModel.checkSecurity();
        AdminClient adminClient = null;
        try {
            adminClient = KafkaAdminClient.create(props);
            KafkaFuture<Set<String>> kafkaFuture = adminClient.listTopics().names();
            kafkaFuture.get(10, TimeUnit.SECONDS);
            System.out.println("----------------------------------" + kafkaFuture);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
    }


}
