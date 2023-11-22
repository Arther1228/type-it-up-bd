package com.yang.hbase.demo.phoenix;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @explain JAVA反射工具类
 * @author Song
 * @date 2019/12/17
 */
public class ReflectionUtil {


    /**
     * 创建Phoenix连接
     * @param ip zookeeper地址
     * @param hadoopCommonUrl HadoopCommon的url
     * @param schema 查询的Schema前缀（没有可为空字符串）
     * @return 成功后的连接信息
     * @throws ClassNotFoundException E
     * @throws SQLException E
     */
    public java.sql.Connection connectionPhoenix(String ip, String hadoopCommonUrl, String schema) throws ClassNotFoundException, SQLException {
        System.setProperty("hadoop.home.dir", hadoopCommonUrl);
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        java.sql.Connection conn = DriverManager.getConnection("jdbc:phoenix:" + ip);
        if (!StringUtils.isEmpty(schema)) {
            conn.setSchema(schema);
        }
        return conn;
    }

    /**
     * 发送SQL语句查询信息
     * @param conn 创建好的Phoenix连接
     * @param schema 要查询的所属Schema（如果有前缀就就加，没有就不加）
     * @param sql 要发送的SQL语句（select * from tableName）
     * @return 查询后的结果
     * @throws SQLException E
     */
    public static ResultSet sqlPhoenix(java.sql.Connection conn, String schema, String sql) throws SQLException {
        conn.setSchema(schema);
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }



    /**
     * 获取私有成员变量的值
     * @param instance 要获取的对象
     * @param filedName 获取的变量名称
     * @return 返回获取变量的信息（需要强转）
     */
    public static Object getPrivateField(Object instance, String filedName) throws NoSuchFieldException, IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(filedName);
        field.setAccessible(true);
        return field.get(instance);
    }

    /**
     * 设置私有成员的值
     * @param instance 要获取的对象
     * @param fieldName 要获取的变量名
     * @param value 设置的值
     */
    public static void setPrivateField(Object instance, String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }

    /**
     * 访问私有方法
     * @param instance 要获取的对象
     * @param methodName 私有方法的名称
     * @param classes  CLASS的返回信息
     * @param objects 参数信息
     * @return
     */
    public static Object invokePrivateMethod(Object instance, String methodName, Class[] classes, String objects) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method method = instance.getClass().getDeclaredMethod(methodName, classes);
        method.setAccessible(true);
        return method.invoke(instance, objects);
    }

}

