package com.yang.kerberos.demo.hbase;

import org.junit.Test;

import java.io.IOException;

/**
 * @author yangliangchuang 2023/10/8 10:46
 */
public class HbaseAPITest {

    private static final String COLUMNS_FAMILY_1 = "cf1";
    private static final String COLUMNS_FAMILY_2 = "cf2";

    private static String table = "student";



    @Test
    public void tableList() throws IOException {
        HDHbaseAPI.tableList();
    }

    @Test
    public void getTableRegions() throws IOException {
        String tableName = "WIFI";
        HDHbaseAPI.getTableRegions(tableName);
    }


}
