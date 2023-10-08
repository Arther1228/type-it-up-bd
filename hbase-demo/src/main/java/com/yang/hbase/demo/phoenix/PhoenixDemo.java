package com.yang.hbase.demo.phoenix;

import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PhoenixDemo {


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
        Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");
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
}
