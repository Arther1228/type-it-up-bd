package com.yang.clickhouse.demo;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yangliangchuang 2023/12/20 13:35
 */
@Slf4j
public class HDClickhouseTest {

    public static final String passedList = "select t.*  " +
            "FROM shiny.vehicle_full_replica_dist t " +
            "WHERE t.pass_time >= toDateTime( ? ) " +
            "and t.pass_time <= toDateTime( ? ) " +
            "limit ?, ?";

    public static final String countSQL = "select count(1) as count  " +
            "FROM shiny.vehicle_full_replica_dist t " +
            "WHERE t.pass_time >= toDateTime( ? ) " +
            "and t.pass_time <= toDateTime( ? ) ";


    @Test
    public void getCount() throws SQLException {
        Instant startTime = Instant.now();

        int sum = 0;
        // 连接 ClickHouse
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://34.8.8.106:8123/shiny", "default", "");

        Object[] params = new Object[]{"2023-12-14 00:00:00", "2023-12-15 00:00:00"};
        PreparedStatement preparedStatement = connection.prepareStatement(countSQL);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }

        ResultSet resultSet = preparedStatement.executeQuery();
        // 遍历结果
        while (resultSet.next()) {
            String count = resultSet.getString("count");
            sum = Integer.valueOf(count);
        }

        System.out.println(sum);

        Instant endTime = Instant.now();
        long seconds = ChronoUnit.MILLIS.between(startTime, endTime);
        log.info("查询数据总量耗时(毫秒)：{}，数据总量：{}", seconds, sum);



    }

    @Test
    public void getDataList() throws SQLException {

        List<String> result = new ArrayList<>();

        Instant startTime = Instant.now();
        int count = 10000;

        // 连接 ClickHouse
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://34.8.8.106:8123/shiny", "default", "");

        Object[] params = new Object[]{"2023-12-14 00:00:00", "2023-12-15 00:00:00", 0, count};
        PreparedStatement preparedStatement = connection.prepareStatement(passedList);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }

        ResultSet resultSet = preparedStatement.executeQuery();
        // 遍历结果
        while (resultSet.next()) {
            String plate_no = resultSet.getString("plate_no");
            result.add(plate_no);
        }

        System.out.println(result.size());

        Instant endTime = Instant.now();
        long seconds = ChronoUnit.SECONDS.between(startTime, endTime);
        log.info("生成 {} 条数据消耗时间(秒)：{}", count, seconds);


    }


}
