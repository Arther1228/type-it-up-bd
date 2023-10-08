package com.yang.hbase.demo.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author arvin
 * Windows Hbase Docker实践 https://blog.csdn.net/qq_34387470/article/details/120776587
 */
public class HbaseAPI {
    private static Admin admin;
    private static final String COLUMNS_FAMILY_1 = "cf1";
    private static final String COLUMNS_FAMILY_2 = "cf2";

    public static Connection initHbase() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    //创建表 create
    public static void createTable(TableName tableName, String[] cols) throws IOException {
        admin = initHbase().getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("Table Already Exists！");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Table Create Successful");
        }
    }

    public static TableName getTbName(String tableName) {
        return TableName.valueOf(tableName);
    }

    // 删除表 drop
    public static void deleteTable(TableName tableName) throws IOException {
        admin = initHbase().getAdmin();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }

    //put 插入数据
    public static void insertData(TableName tableName, Student student) throws IOException {
        Put put = new Put(Bytes.toBytes(student.getId()));
        put.addColumn(Bytes.toBytes(COLUMNS_FAMILY_1), Bytes.toBytes("name"), Bytes.toBytes(student.getName()));
        put.addColumn(Bytes.toBytes(COLUMNS_FAMILY_1), Bytes.toBytes("age"), Bytes.toBytes(student.getAge()));
        initHbase().getTable(tableName).put(put);
        System.out.println("Data insert success:" + student.toString());
    }

    // delete 删除数据
    public static void deleteData(TableName tableName, String rowKey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
//        delete = delete.addColumn(Bytes.toBytes(COLUMNS_FAMILY_1), Bytes.toBytes("name"));  // 指定column，也可以不指定，删除该rowKey的所有column
        initHbase().getTable(tableName).delete(delete);
        System.out.println("Delete Success");
    }

    // scan数据
    public static List<Student> allScan(TableName tableName) throws IOException {
        ResultScanner results = initHbase().getTable(tableName).getScanner(new Scan().addFamily(Bytes.toBytes("cf1")));
        List<String> list = new ArrayList<>();
        for (Result result : results) {
            Student student = new Student();
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }
        }
        return null;
    }

    // 根据rowkey get数据
    public static Student singleGet(TableName tableName, String rowKey) throws IOException {
        Student student = new Student();
        student.setId(rowKey);
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = initHbase().getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                switch (colName) {
                    case "name":
                        student.setName(value);
                        break;
                    case "age":
                        student.setAge(value);
                        break;
                    default:
                        System.out.println("unknown columns");
                }
            }
        }
        System.out.println(student.toString());
        return student;
    }


    public static void tableList() throws IOException {
        admin = initHbase().getAdmin();

        // Getting all the list of tables using HBaseAdmin object
        HTableDescriptor[] tableDescriptor = admin.listTables();

        // printing all the table names.
        for (int i = 0; i < tableDescriptor.length; i++) {
            System.out.println(tableDescriptor[i].getNameAsString());
        }

    }


    // 查询指定Cell数据
    public static String getCell(TableName tableName, String rowKey, String cf, String column) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        String rst = null;
        if (!get.isCheckExistenceOnly()) {
            get = get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));
            try {
                Result result = initHbase().getTable(tableName).get(get);
                byte[] resByte = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(column));
                rst = Bytes.toString(resByte);
            } catch (Exception exception) {
                System.out.printf("columnFamily or column does not exists");
            }
        }
        System.out.println("Value is: " + rst);
        return rst;
    }

    public static class Student {
        private String id;
        private String name;
        private String age;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}

