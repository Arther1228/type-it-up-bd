package com.yang.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 查询
 */
public class TestHbaseAPI4 {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "myhbase");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("student");

        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("info"));
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("1002"));
        RegexStringComparator rsc = new RegexStringComparator("^\\d{5}$");
//        Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,bc);
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rsc);

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, bc);

        //MUST_PASS_ALL:必须满足所有条件
        //MUST_PASS_ONE:只需要满足一个即可，能显示所有满足条件的数据
        //过滤器的集合
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);

        list.addFilter(filter);
        list.addFilter(rowFilter);

        //扫描时，增加过滤器，即添加查询规则
        //过滤，每条数据都会过滤，性能较低
        scan.setFilter(list);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            //展示数据
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey=" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family=" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column=" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value=" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
        connection.close();

    }
}