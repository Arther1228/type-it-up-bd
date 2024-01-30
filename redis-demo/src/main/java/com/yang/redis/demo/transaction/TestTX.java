package com.yang.redis.demo.transaction;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * Rdis 事务
 * @author wqj24
 *
 * 使用Java操作Redis事务 https://www.jianshu.com/p/de96ea75f530
 *
 */
public class TestTX {

    static final Jedis jedis = new Jedis("132.232.6.208", 6381);
    /**
     * 清空数据库
     */
    @Before
    public void flushdb() {
        jedis.flushDB();
    }

    @Test
    public void commitTest() {

        // 开启事务
        Transaction transaction = jedis.multi();

        transaction.set("key-1", "value-1");
        transaction.set("key-2", "value-2");
        transaction.set("key-3", "value-3");
        transaction.set("key-4", "value-4");

        // 提交事务
        transaction.exec();

        System.out.println(jedis.keys("*"));
    }


    @Test
    public void discardTest() {

        // 开启事务
        Transaction transaction = jedis.multi();

        transaction.set("key-1", "value-1");
        transaction.set("key-2", "value-2");
        transaction.set("key-3", "value-3");
        transaction.set("key-4", "value-4");

        // 放弃事务
        transaction.discard();

        System.out.println(jedis.keys("*"));
    }

    /**
     * watch 命令会标记一个或多个键
     * 如果事务中被标记的键，在提交事务之前被修改了，那么事务就会失败。
     * @return
     * @throws InterruptedException
     */
    @Test
    public void watchTest() throws InterruptedException {
        boolean resultValue = transMethod(10);
        System.out.println("交易结果（事务执行结果）：" + resultValue);

        int balance = Integer.parseInt(jedis.get("balance"));
        int debt = Integer.parseInt(jedis.get("debt"));

        System.out.printf("balance: %d, debt: %d\n", balance, debt);
    }

    // 支付操作
    public static boolean transMethod(int amtToSubtract) throws InterruptedException {
        int balance;  // 余额
        int debt;  // 负债

        jedis.set("balance", "100");
        jedis.set("debt", "0");

        jedis.watch("balance", "debt");

        balance = Integer.parseInt(jedis.get("balance"));

        // 余额不足
        if (balance < amtToSubtract) {
            jedis.unwatch();  // 放弃所有被监控的键
            System.out.println("Insufficient balance");

            return false;
        }

        Transaction transaction = jedis.multi();
        // 扣钱
        transaction.decrBy("balance", amtToSubtract);
        Thread.sleep(5000);  // 在外部修改 balance 或者 debt
        transaction.incrBy("debt", amtToSubtract);

        // list为空说明事务执行失败
        List<Object> list = transaction.exec();

        return !list.isEmpty();
    }
}
