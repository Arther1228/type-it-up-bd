package com.yang.kerberos.demo.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SecurityModel {

    private static final Logger log = LoggerFactory.getLogger(SecurityModel.class);

    /**
     * 用户自己申请的机机账号keytab文件名称
     */
    private static final String USER_KEYTAB_FILE = "user.keytab";

    /**
     * 用户自己申请的机机账号名称
     */
    private static final String USER_PRINCIPAL = "zx_test";


    /**
     * kafka安全认证
     */
    public static void checkSecurity(){
        //kafka安全认证
        if (isSecurityModel())
        {
            try
            {
                log.info("Securitymode start.");

                //!!注意，安全认证时，需要用户手动修改为自己申请的机机账号
                securityPrepare();
            }
            catch (IOException e)
            {
                log.error("Security prepare failure.");
                log.error("The IOException occured : {}.", e);
                return;
            }
            log.info("Security prepare success.");
        }
    }

    public static Boolean isSecurityModel()
    {
        Boolean isSecurity = false;
        String krbFilePath = System.getProperty("user.dir") + File.separator  + "kerberos-demo" + File.separator + "config" + File.separator + "kafkaSecurityMode";

        Properties securityProps = new Properties();

        // file does not exist.
        if (!isFileExists(krbFilePath))
        {
            return isSecurity;
        }

        try
        {
            securityProps.load(new FileInputStream(krbFilePath));
            if ("yes".equalsIgnoreCase(securityProps.getProperty("kafka.client.security.mode")))
            {
                isSecurity = true;
            }
        }
        catch (Exception e)
        {
            log.info("The Exception occured : {}.", e);
        }

        return isSecurity;
    }

    /*
     * 判断文件是否存在
     */
    private static boolean isFileExists(String fileName)
    {
        File file = new File(fileName);

        return file.exists();
    }

    public static void securityPrepare() throws IOException
    {
        String filePath =  System.getProperty("user.dir") + File.separator  + "kerberos-demo" + File.separator + "config" + File.separator;
//        String filePath =  File.separator + "opt" + File.separator + "conf" + File.separator;
        String krbFile = filePath + "krb5.conf";
        String userKeyTableFile = filePath + USER_KEYTAB_FILE;

        log.info("filePath: " + filePath);

        //windows路径下分隔符替换
        userKeyTableFile = userKeyTableFile.replace("\\", "\\\\");
        krbFile = krbFile.replace("\\", "\\\\");

        LoginUtil.setKrb5Config(krbFile);
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.setJaasFile(USER_PRINCIPAL, userKeyTableFile);
    }
}
