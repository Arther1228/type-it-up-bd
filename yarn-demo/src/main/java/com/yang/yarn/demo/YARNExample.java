package com.yang.yarn.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.List;

public class YARNExample {
    public static void main(String[] args) throws IOException, YarnException {
        // 设置Hadoop配置
        Configuration conf = new Configuration();
        conf.set("yarn.resourcemanager.hostname", "localhost"); // 根据您的YARN配置更改主机名

        // 创建YARN客户端
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // 获取YARN集群的信息
        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        System.out.println("Total Nodes: " + clusterMetrics.getNumNodeManagers());

        // 获取队列信息
        List<QueueInfo> queueInfos = yarnClient.getAllQueues();
        System.out.println("Queue Information:");
        for (QueueInfo queueInfo : queueInfos) {
            System.out.println("Queue Name: " + queueInfo.getQueueName());
            System.out.println("Queue Capacity: " + queueInfo.getCapacity());
        }

        // 关闭YARN客户端
        yarnClient.stop();
    }
}
