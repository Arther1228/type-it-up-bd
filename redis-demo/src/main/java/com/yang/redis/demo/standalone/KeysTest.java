package com.yang.redis.demo.standalone;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author yangliangchuang 2022/11/10 15:58
 * Jedis使用scan 模糊删除匹配的key
 * https://blog.csdn.net/qq_39313596/article/details/108642209
 *
 * 备注：
 * （1）windows单机版redis 测试通过
 * （2）只能默认删除第一个库的key
 */
public class KeysTest {

    private static Jedis jedis;

    static {
        jedis = new Jedis("127.0.0.1", 6379);
    }

    private Jedis getJedis() {
        return jedis;
    }

    //批量获取匹配的所有的key
    public Set<String> getScan(String key, Integer count) {
        Jedis jedis = getJedis();
        Set<String> sets = new HashSet<>();
        ScanParams params = new ScanParams();
        params.match(key);
        params.count(count);
        String cursor = "0";
        while (true) {
            ScanResult scanResult = jedis.scan(cursor, params);
            List<String> elements = scanResult.getResult();
            if (elements != null && elements.size() > 0) {
                sets.addAll(elements);
            }
            cursor = scanResult.getStringCursor();
            if ("0".equals(cursor)) {
                break;
            }
        }
        returnJedis(jedis);
        return sets;
    }


    //删除 批量key
    public boolean delLargeHashKey(String key, Integer count) {
        Jedis jedis = getJedis();
        //TODO 选择数据库删除失败
        jedis.select(5);
        try {
            Set<String> sets = getScan(key, count);
            if (sets != null && sets.size() > 0) {
                for (String keyt : sets) {
                    //删除
                    Long del = jedis.del(keyt);
                    System.out.println(del);
                }
                return true;
            }
            return false;
        } catch (JedisException e) {
            if (jedis != null) {
                returnJedis(jedis);
            }
            return false;
        } finally {
            if (jedis != null) {
                returnJedis(jedis);
            }
        }
    }

    private void returnJedis(Jedis jedis) {
        jedis.close();
    }


    @Test
    public void test1() {
        boolean b = delLargeHashKey("/*", 1);
        System.out.println(b);
    }
}
