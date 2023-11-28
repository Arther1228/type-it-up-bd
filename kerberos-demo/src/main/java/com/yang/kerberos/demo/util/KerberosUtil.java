package com.yang.kerberos.demo.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.File;
import java.util.Properties;

/**
 * @author yangliangchuang 2023-11-28 16:33
 */
@Slf4j
public class KerberosUtil {

    public static AdminClient createKafkaClient() {
        AdminClient adminClient = null;
        try {
            String krb5ConfPath = System.getProperty("user.dir") + File.separator + "kerberos-demo" + "/conf/" + "krb5.conf";
            String jaasConfPath = System.getProperty("user.dir") + File.separator + "kerberos-demo" + "/conf/" + "jaas.conf";

            System.setProperty("java.security.krb5.conf", krb5ConfPath);
            System.setProperty("sun.security.krb5.debug", "true");
            System.setProperty("java.security.auth.login.config", jaasConfPath);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.8.8.115:21007");
//        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-connection");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "GSSAPI");
            props.put("sasl.kerberos.service.name", "kafka");
            adminClient = AdminClient.create(props);
        } catch (Exception e) {
            log.error("error:{}", e.getMessage());
        }

        return adminClient;
    }
}
