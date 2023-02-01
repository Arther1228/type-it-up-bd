package com.yang.spark2x.demo.util;

import org.junit.Test;

import java.io.*;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * @author yangliangchuang 2022/11/20 17:26
 */
public class FileUtil {

    /** 方式一 **/
    /**
     * 本地打印： sun.misc.Launcher$AppClassLoader@18b4aac2
     * 服务器打印： org.apache.spark.util.MutableURLClassLoader@23282c25
     * 结果：（1）本地：读出成功；（2）服务器：成功读出；
     */
    public void getResourceByClassLoader1() {
        try {
            ClassLoader classLoader = FileUtil.class.getClassLoader();
            System.out.println("FileUtil.class.getClassLoader() 获得的加载器为: " + classLoader);

            //1.src平级目录
            InputStream in = classLoader.getResourceAsStream("conf/a.properties");
            printValue(in, "a");

        } catch (Exception e) {
            System.out.println("FileUtil.class.getResourceAsStream(filePath)方式读取文件内容失败");
        }
    }

    /** 方式二**/
    /**
     * 本地打印： sun.misc.Launcher$AppClassLoader@18b4aac2
     * 服务器打印： sun.misc.Launcher$AppClassLoader@e9e54c2
     * 结果：（1）本地：成功读出；（2）服务器：读出失败；
     */
    public void getResourceByClassLoader2() {
        try {
            ClassLoader classLoader = ClassLoader.getSystemClassLoader();
            System.out.println("ClassLoader.getSystemClassLoader() 获得的加载器为: " + classLoader);

            //1.src平级目录
            InputStream in = classLoader.getResourceAsStream("conf/a.properties");
            printValue(in, "a");

        } catch (Exception e) {
            System.out.println("ClassLoader.getSystemClassLoader().getResourceAsStream(filePath)方式读取文件内容失败");
        }
    }

    /** 方式三**/
    /**
     * 结果：（1）本地：@Test正常，main函数读出失败；（2）服务器：读出失败；
     * 很奇怪的问题：main函数中运行报错，@Test运行正常；
     */
    @Test
    public void getResourceByInputStream() {
        try {
            //1.src平级目录
            InputStream resourceAsStreamA = new FileInputStream(new File("conf/a.properties"));
            printValue(resourceAsStreamA, "a");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("getResourceByInputStream方式读取文件内容失败");
        }
    }

    /** 方式四**/
    /**
     * 结果：（1）本地：成功读出；（2）服务器：成功读出；
     */
    @Test
    public void getResourceByResourceBundle() {
        try {
            //1.src平级目录
            ResourceBundle resourceBundleA = ResourceBundle.getBundle("conf/a");
            printValue(resourceBundleA, "a");
        } catch (Exception e) {
            System.out.println("getResourceByResourceBundle方式读取文件内容失败");
        }
    }

    private void printValue(ResourceBundle resourceBundle, String key) {
        String a = resourceBundle.getString(key);
        System.out.println("src下资源文件：" + a);
    }

    /**
     * 通过InputStream拿到key对应的value值
     *
     * @param resourceAsStream
     * @param key
     */
    private void printValue(InputStream resourceAsStream, String key) {
        Properties properties = new Properties();
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String a = properties.getProperty(key);
        System.out.println(a);
    }

    public String collectTest() {
        getResourceByClassLoader1();
//        getResourceByClassLoader2();
//        getResourceByInputStream();
//        getResourceByResourceBundle();
        return "============123================";
    }


    public static void main(String[] args) {

        FileUtil fileUtil = new FileUtil();
        fileUtil.collectTest();

    }
}
