package com.yang.flink.demo.datasource;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.*;

public class MysqlRichParallelSource extends RichParallelSourceFunction<UserInfo> {


    private boolean close = false;

    @Override
    public void run(SourceContext<UserInfo> out) throws Exception {
        String url = "jdbc:mysql://localhost:3306/world?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false&allowMultiQueries=true&rewriteBatchedStatements=true";
        String sql = "select * from user_info";
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection(url, "root", "abc.123");
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        while (!close) {
            rs = ps.executeQuery();
            while (rs.next()) {
                Integer userId = rs.getInt("user_id");
                String userName = rs.getString("user_name");
                String userRealName = rs.getString("user_real_name");
                UserInfo user = new UserInfo();
                user.setUserId(userId);
                user.setUserName(userName);
                user.setUserRealName(userRealName);
                //收集数据
                out.collect(user);
            }
            Thread.sleep(5000);
            cancel();
        }
        close(conn, ps, rs);
    }

    @Override
    public void cancel() {
        close = true;
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
