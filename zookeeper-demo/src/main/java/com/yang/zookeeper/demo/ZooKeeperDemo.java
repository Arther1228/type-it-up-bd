package com.yang.zookeeper.demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZooKeeperDemo {


    @Test
    public void testZKAll() throws Exception {
        ls("/");
    }

    /**
     * 列出指定path下所有孩子
     */
    public void ls(String path) throws Exception {
        System.out.println(path);
        ZooKeeper zk = new ZooKeeper("localhost:2181",5000,null);
        List<String> list = zk.getChildren(path,null);
        //判断是否有子节点
        if(list.isEmpty() || list == null){
            return;
        }
        for(String s : list){
            //判断是否为根目录
            if(path.equals("/")){
                ls(path + s);
            }else {
                ls(path +"/" + s);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String zkConnectString = "localhost:2181"; // ZooKeeper服务器地址和端口
        int sessionTimeout = 3000; // 会话超时时间（毫秒）

        ZooKeeper zooKeeper = new ZooKeeper(zkConnectString, sessionTimeout, null);

        // 创建一个ZooKeeper节点
        String path = "/myNode";
        String data = "Hello, ZooKeeper!";
        zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // 读取节点数据
        byte[] nodeData = zooKeeper.getData(path, false, null);
        System.out.println("Node data: " + new String(nodeData));

        // 修改节点数据
        String newData = "Updated data!";
        zooKeeper.setData(path, newData.getBytes(), -1);

        // 读取修改后的节点数据
        nodeData = zooKeeper.getData(path, false, null);
        System.out.println("Updated node data: " + new String(nodeData));

        // 删除节点
//        zooKeeper.delete(path, -1);

        // 检查节点是否存在
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            System.out.println("Node " + path + " does not exist.");
        }

        // 关闭ZooKeeper连接
        zooKeeper.close();
    }

    @Test
    public void getAllData() throws IOException, KeeperException, InterruptedException {
        String zkConnectString = "localhost:2181"; // ZooKeeper服务器地址和端口
        int sessionTimeout = 3000; // 会话超时时间（毫秒）

        ZooKeeper zooKeeper = new ZooKeeper(zkConnectString, sessionTimeout, null);

        // 获取根节点下的所有子节点
        List<String> children = zooKeeper.getChildren("/", false);

        // 遍历子节点并获取它们的数据
        for (String child : children) {
            String nodePath = "/" + child;
            Stat stat = zooKeeper.exists(nodePath, false);

            if (stat != null) {
                byte[] nodeData = zooKeeper.getData(nodePath, false, null);
                System.out.println("Node: " + nodePath + ", Data: " + new String(nodeData));
            } else {
                System.out.println("Node " + nodePath + " does not exist.");
            }
        }

        // 关闭ZooKeeper连接
        zooKeeper.close();

    }


}
