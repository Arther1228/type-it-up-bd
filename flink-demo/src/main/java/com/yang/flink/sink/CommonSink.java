package com.yang.flink.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.persistence.Column;
import java.lang.reflect.Field;
import java.sql.*;

/**
 * flink 实现CommonSink，自动生成sql，自动匹配字段值
 * https://blog.csdn.net/devcy/article/details/116023780
 *
 * @param <T>
 */
public class CommonSink<T> extends RichSinkFunction<T> {
    PreparedStatement ps;
    Connection connection;
    String tn;

    public CommonSink(String tn) {
        this.tn = tn;
    }

    @Override
    public void invoke(T t, Context context) throws Exception {

        String url = "jdbc:mysql://localhost:3306/world?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false&allowMultiQueries=true&rewriteBatchedStatements=true";
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = DriverManager.getConnection(url, "root", "abc.123");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Class<?> c = t.getClass();
        Field[] fs = c.getDeclaredFields();
        StringBuilder sc = new StringBuilder(" (");
        StringBuilder sq = new StringBuilder(" ) values (");

        for (Field f : fs) {
            String dbFieldName = "";

            if (f.isAnnotationPresent(Column.class)) {
                Column column = f.getAnnotation(Column.class);
                if (column != null) {
                    dbFieldName = column.name();
                }
            }

            if (StringUtils.isNotEmpty(dbFieldName)) {
                sc.append(dbFieldName).append(",");
                sq.append("? ,");
            }
        }
        StringBuilder sb = new StringBuilder("replace into ");
        sb.append(tn).append(sc.toString(), 0, sc.toString().length() - 1).append(sq.toString(), 0, sq.toString().length() - 1).append(")");
        ps = connection.prepareStatement(sb.toString());
        for (int i = 1; i <= fs.length; i++) {
            fs[i - 1].setAccessible(true);
            ps.setObject(i, fs[i - 1].get(t));
        }
        ps.executeUpdate();

        close(conn, ps, rs);
    }

    public void close(Connection conn, PreparedStatement ps, ResultSet rs) throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
        if (rs != null) {
            rs.close();
        }
    }
}
