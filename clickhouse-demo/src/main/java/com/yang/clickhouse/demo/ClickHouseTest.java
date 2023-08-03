package com.yang.clickhouse.demo;

import com.google.common.collect.Lists;
import org.junit.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * https://www.cnblogs.com/throwable/p/14015092.html
 */
public class ClickHouseTest {

    @Test
    public void testCh() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser("root");
        props.setPassword("root");
        // 不创建数据库的时候会有有个全局default数据库
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123/default", props);
        ClickHouseConnection connection = dataSource.getConnection();
        ClickHouseStatement statement = connection.createStatement();
        // 创建一张表，表引擎为Memory，这类表在服务重启后会自动删除
        boolean execute = statement.execute("CREATE TABLE IF NOT EXISTS t_test(id UInt64,name String) ENGINE  = Memory");
        if (execute) {
            System.out.println("创建表default.t_test成功");
        } else {
            System.out.println("表default.t_test已经存在");
        }
        ResultSet rs = statement.executeQuery("SHOW TABLES");
        List<String> tables = Lists.newArrayList();
        while (rs.next()) {
            tables.add(rs.getString(1));
        }
        System.out.println("default数据库中的表:" + tables);
        PreparedStatement ps = connection.prepareStatement("INSERT INTO t_test(*) VALUES (?,?),(?,?)");
        ps.setLong(1, 1L);
        ps.setString(2, "throwable");
        ps.setLong(3, 2L);
        ps.setString(4, "doge");
        ps.execute();
        statement = connection.createStatement();
        rs = statement.executeQuery("SELECT * FROM t_test");
        while (rs.next()) {
            System.out.println(String.format("查询结果,id:%s,name:%s", rs.getLong("id"), rs.getString("name")));
        }
    }
}
