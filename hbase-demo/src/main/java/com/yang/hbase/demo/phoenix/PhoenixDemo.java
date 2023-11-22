package com.yang.hbase.demo.phoenix;

import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        statement.setString(1,"1002");
        statement.setString(2,"zhangsan2");
        statement.setString(3,"21");

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
     * JAVA使用Phoenix连接操作HBase工具类__获取表字段信息等操作 https://blog.csdn.net/Remember__Peng/article/details/104054914
     * @throws SQLException
     */
    @Test
    public void getTableColums() throws SQLException {

        String tableName = "XXX";
        String schema = "";
        Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");

        try {
            //获取表中的字段信息
            PhoenixResultSet resultSet = (PhoenixResultSet)ReflectionUtil.sqlPhoenix(connection, schema, ("select * from " + tableName));
            //这里用到了JAVA反射，因为字段信息集合在jar依赖中是private类型，所以通过反射来获取到他的字段名称信息【工具类在下方】
            RowProjector row = (RowProjector) ReflectionUtil.getPrivateField(resultSet, "rowProjector");
            List<? extends ColumnProjector> column = row.getColumnProjectors();
            //String[] arry = new String[column.size()];
            for (int i = 0; i < column.size(); i++) {
                System.out.println("这是字段信息 ====>" + column.get(i).getName());
                //arry[i] = column.get(i).getName();
            }
        }catch (PhoenixParserException e){
            System.out.println("不允许查询的：" + tableName);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } finally {
            System.out.println("*****************分割符************");
        }
    }

    /**
     * 获取Phoenix中的表(系统表除外)
     */
    @Test
    public void getTables() throws Exception {
        List<String> tables = new ArrayList<>();
        Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");
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
    public void getDataList() throws Exception {
        String tableName = "XXX";
        String sql = "SELECT * FROM " + tableName;
        Connection connection = DriverManager.getConnection("jdbc:phoenix:127.0.0.1:2181");
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
