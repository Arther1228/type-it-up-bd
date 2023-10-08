package com.yang.hbase.demo.client;

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
    public void createTableTest() throws IOException {
        HbaseAPI.createTable(HbaseAPI.getTbName(table), new String[]{COLUMNS_FAMILY_1, COLUMNS_FAMILY_2});
    }

    @Test
    public void tableList() throws IOException {
        HbaseAPI.tableList();
    }

    @Test
    public void getTbName() {
    }

    @Test
    public void deleteTableTest() throws IOException {

        HbaseAPI.deleteTable(HbaseAPI.getTbName(table));
    }

    @Test
    public void insertDataTest() throws IOException {

        HbaseAPI.Student student = new HbaseAPI.Student();
        student.setId("1");
        student.setName("Arvin");
        student.setAge("18");
        HbaseAPI.insertData(HbaseAPI.getTbName(table), student);

    }

    @Test
    public void singleGetTest() throws IOException {

        HbaseAPI.singleGet(HbaseAPI.getTbName(table), "2");
    }


    @Test
    public void allScanTest() {
    }

    @Test
    public void deleteDataTest() throws IOException {

        HbaseAPI.deleteData(HbaseAPI.getTbName(table), "1");

    }

    @Test
    public void getCellTest() throws IOException {

        HbaseAPI.getCell(HbaseAPI.getTbName(table), "2", "cf1", "name");

    }
}
