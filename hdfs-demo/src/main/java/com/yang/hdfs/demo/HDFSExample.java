package com.yang.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;

public class HDFSExample {
    public static void main(String[] args) throws IOException {
        // 设置Hadoop配置
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); // 根据您的HDFS配置更改此处的URL

        // 创建HDFS文件系统对象
        FileSystem fs = FileSystem.get(conf);

        // 演示创建目录
        Path newDir = new Path("/example");
        fs.mkdirs(newDir);

        // 演示创建文件并写入内容
        Path filePath = new Path("/example/test.txt");
        FSDataOutputStream outputStream = fs.create(filePath);
        outputStream.writeUTF("Hello, HDFS!");
        outputStream.close();

        // 演示读取文件内容
        FSDataInputStream inputStream = fs.open(filePath);
        String content = inputStream.readUTF();
        System.out.println("File Content: " + content);
        inputStream.close();

        // 演示删除文件
//        fs.delete(filePath, false);

        // 演示删除目录（需要空目录）
//        fs.delete(newDir, true);

        // 关闭文件系统连接
        fs.close();
    }
}
