package com.yang.hbase.demo;

import java.io.IOException;

/**
 * 测试HBase API
 * https://www.rossontheway.com/2019/12/23/%E4%BD%BF%E7%94%A8Docker%E9%83%A8%E7%BD%B2HBase%E5%B9%B6%E4%BD%BF%E7%94%A8Java-API%E8%BF%9E%E6%8E%A5/
 */
public class TestHbaseAPI3 {

    public static void main(String[] args) throws IOException {

        //创建连接对象
        HBaseUtil.makeHBaseConnection("myhbase", "2181");

        HBaseUtil.insertData("ross:student", "1003", "info", "name", "jinqi");

        //关闭连接
        HBaseUtil.close();
    }
}