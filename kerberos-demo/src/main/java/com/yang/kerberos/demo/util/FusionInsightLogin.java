package com.yang.kerberos.demo.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * 登陆到华为大数据平台，安全认证机制
 *
 * @author guanmaogao 2019年3月18号 15:57
 */
public class FusionInsightLogin {

    private static final Logger LOG = LoggerFactory.getLogger(FusionInsightLogin.class);

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static Configuration conf = null;
    private static String krb5File = null;

    private static String userName = "zx_test";

    private static String url = "phoenix";

    private static String userKeytabFile = null;

    public static void initAndLogin() {
        try {
            // 华为大数据平台的登陆认证,其他不需要认证
            if (StringUtils.isEmpty(url) || url.equals("phoenix")) {
                init();
                login();
            }
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

    }

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            userKeytabFile = userdir + "user.keytab";
            krb5File = userdir + "krb5.conf";

            /*
             * if need to connect zk, please provide jaas info about zk. of course, you can
             * do it as below: System.setProperty("java.security.auth.login.config",
             * confDirPath + "jaas.conf"); but the demo can help you more : Note: if this
             * process will connect more than one zk cluster, the demo may be not proper.
             * you can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
    }

}
