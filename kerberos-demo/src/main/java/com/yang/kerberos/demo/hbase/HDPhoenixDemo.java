package com.yang.kerberos.demo.hbase;

import com.yang.kerberos.demo.util.FusionInsightLogin;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDPhoenixDemo {


    @Test
    public void createTable() throws Exception{
        //2、获取连接
        Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");
        //3、创建Statement对象
        String sql = "create table xxx(" +
                "id varchar primary key," +
                "name varchar," +
                "age varchar)COLUMN_ENCODED_BYTES=0";
        PreparedStatement statement = connection.prepareStatement(sql);
        //4、执行sql操作
        statement.execute();
        //5、关闭
        statement.close();
        connection.close();
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void insert() throws Exception{
        //2、获取连接
        Connection connection = DriverManager.getConnection("jdbc:phoenix:34.8.8.4:24002");
        //connection.setAutoCommit(true);
        //3、获取statement对象
        PreparedStatement statement = connection.prepareStatement("upsert into xxx values(?,?,?)");

        //4、给参数赋值
        statement.setString(1,"1001");
        statement.setString(2,"zhangsan");
        statement.setString(3,"20");

        //5、执行插入,需要手动提交，不然无法插入
        statement.execute();
        connection.commit();
        //6、关闭
        statement.close();
        connection.close();
    }

    @Test
    public void query() throws Exception{
        //2、获取连接
        Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");
        //connection.setAutoCommit(true);
        //3、获取statement对象
        PreparedStatement statement = connection.prepareStatement("select * from xxx");

        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()){
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String age = resultSet.getString("age");
            System.out.println("id="+id+",name="+name+",age="+age);
        }

        statement.close();
        connection.close();
    }

    /**
     * 获取Phoenix中的表(系统表除外)
     */
    @Test
    public void getTables() throws Exception {

        FusionInsightLogin.initAndLogin();

        List<String> tables = new ArrayList<>();
        Connection connection = DriverManager.getConnection("jdbc:phoenix:34.8.8.4:24002");
        DatabaseMetaData metaData = connection.getMetaData();
        String[] types = {"TABLE"}; //"SYSTEM TABLE"
        ResultSet resultSet = metaData.getTables(null, null, null, types);
        while (resultSet.next()) {
            tables.add(resultSet.getString("TABLE_NAME"));
        }

        tables.stream().forEach(System.out::println);
    }

    /**
     * 获取表中的所有数据
     */
    @Test
    public void getList() throws Exception {

        FusionInsightLogin.initAndLogin();

        String tableName = "TEST";
        String sql = "SELECT * FROM " + tableName;
        Connection connection = DriverManager.getConnection("jdbc:phoenix:34.8.8.4:24002");
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Map<String, String>> resultList = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, String> result = new HashMap<>();
            for (int i = 1, len = resultSetMetaData.getColumnCount(); i <= len; i++) {
                result.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
            }
            resultList.add(result);
        }
        resultList.stream().forEach(System.out::println);
    }

}
