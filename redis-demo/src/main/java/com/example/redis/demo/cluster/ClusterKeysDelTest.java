package com.example.redis.demo.cluster;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.util.JedisClusterCRC16;

import java.util.*;

/**
 * @author yangliangchuang 2022/11/10 18:32
 * Redis & Redis Cluster 字段模糊匹配及删除
 * https://blog.csdn.net/u010416101/article/details/80754171
 */
@Slf4j
public class ClusterKeysDelTest {

    private static JedisCluster jedisCluster;

    static {
        // 添加集群的服务节点Set集合
        Set<HostAndPort> hostAndPortsSet = new HashSet<HostAndPort>();
        // 添加节点
        hostAndPortsSet.add(new HostAndPort("34.8.8.138", 22400));
        hostAndPortsSet.add(new HostAndPort("34.8.8.139", 22400));
        hostAndPortsSet.add(new HostAndPort("34.8.8.140", 22400));
        hostAndPortsSet.add(new HostAndPort("34.8.8.141", 22400));
        hostAndPortsSet.add(new HostAndPort("34.8.8.142", 22400));
        hostAndPortsSet.add(new HostAndPort("34.8.8.143", 22400));

        // Jedis连接池配置
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最大空闲连接数, 默认8个
        jedisPoolConfig.setMaxIdle(100);
        // 最大连接数, 默认8个
        jedisPoolConfig.setMaxTotal(500);
        //最小空闲连接数, 默认0
        jedisPoolConfig.setMinIdle(0);
        // 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
        jedisPoolConfig.setMaxWaitMillis(2000); // 设置2秒
        //对拿到的connection进行validateObject校验
        jedisPoolConfig.setTestOnBorrow(true);
        jedisCluster = new JedisCluster(hostAndPortsSet, jedisPoolConfig);
    }

    @Test
    public void test1() {
        deleteRedisKeyStartWith("downloader-xlxk:");
    }

    public static void deleteRedisKeyStartWith(String redisKeyStartWith) {
        try {
            Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();

            for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
                Jedis jedis = entry.getValue().getResource();
                // 判断非从节点(因为若主从复制，从节点会跟随主节点的变化而变化)
                if (!jedis.info("replication").contains("role:slave")) {
                    Set<String> keys = jedis.keys(redisKeyStartWith + "*");
                    if (keys.size() > 0) {
                        Map<Integer, List<String>> map = new HashMap<>();
                        for (String key : keys) {
                            // cluster模式执行多key操作的时候，这些key必须在同一个slot上，不然会报:JedisDataException:
                            // CROSSSLOT Keys in request don't hash to the same slot
                            int slot = JedisClusterCRC16.getSlot(key);
                            // 按slot将key分组，相同slot的key一起提交
                            if (map.containsKey(slot)) {
                                map.get(slot).add(key);
                            } else {
                                map.put(slot, Lists.newArrayList(key));
                            }
                        }
                        for (Map.Entry<Integer, List<String>> integerListEntry : map.entrySet()) {
                            jedis.del(integerListEntry.getValue().toArray(new String[integerListEntry.getValue().size()]));
                        }
                    }
                }
            }
            log.info("success deleted redisKeyStartWith:{}", redisKeyStartWith);
        } finally {
        }
    }

    /**
     * 测试通过
     */
    @Test
    public void test2() {
        TreeSet<String> keys = keys("custom-rest-api:*");
        //遍历key  进行删除  可以用多线程
        for (String key : keys) {
//            jedisCluster.del(key);
            System.out.println(key);
        }
    }

    /**
     * ### 未使用slot批次提交(有可能效率略差于前者)
     * //@param pattern  获取key的前缀  全是是 *
     *
     * @param pattern
     * @return
     */
    public static TreeSet<String> keys(String pattern) {
        TreeSet<String> keys = new TreeSet<>();
        //获取所有的节点

        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        //遍历节点 获取所有符合条件的KEY

        for (String k : clusterNodes.keySet()) {
            //log.debug("Getting keys from: {}", k);
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                Set<String> resultKeys = connection.keys(pattern);
                if(resultKeys.size() > 0){
                    String nodeInfo = connection.getClient().getSocket().toString();
                    System.out.println(nodeInfo);
                    keys.addAll(resultKeys);
                }

            } catch (Exception e) {
                log.error("Getting keys error: {}", e);
            } finally {
                //log.debug("Connection closed.");
                connection.close();//用完一定要close这个链接！！！
            }
        }
        log.debug("Keys gotten!");
        return keys;
    }


}
