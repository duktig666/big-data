package cn.duktig.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * description:测试 HBase 的DDL操作
 *
 * @author RenShiWei
 * Date: 2021/11/7 17:45
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class HBaseAPI {

    private Connection connection = null;
    private Admin admin = null;

    /**
     * 初始化配置
     */
    public void init() {
        //使用HBaseConfiguration 的单例方法实例化
        Configuration conf = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     */
    public void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @return 表是否存在
     */
    public boolean isTableExist(String tableName) {
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建表
     *
     * @param tableName      表明
     * @param columnFamilies 列族列表
     */
    public void createTable(String tableName, String... columnFamilies) {
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamilies) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            try {
                admin.createTable(descriptor);
                System.out.println("表" + tableName + "创建成功！");
            } catch (IOException e) {
                System.out.println("表" + tableName + "创建失败！");
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除表
     */
    public void dropTable(String tableName) {
        if (isTableExist(tableName)) {
            try {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                System.out.println("表" + tableName + "删除失败！");
                e.printStackTrace();
            }
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }

    /**
     * 向表中插入数据
     *
     * @param tableName    表
     * @param rowKey       行键
     * @param columnFamily 列族
     * @param column       列名
     * @param value        值
     */
    public void put(String tableName, String rowKey, String columnFamily, String column, String value) {
        try {
            // 创建HTable对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            // 创建put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            //向Put对象中组装数据
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取所有行数据
     */
    public void scan(String tableName) {
        // 创建HTable对象
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner scanner = table.getScanner(new Scan());
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    //得到rowkey
                    System.out.println(" 行 键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
                    //得到列族
                    System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取某一行数据
     */
    public void get(String tableName, String rowKey) {
        // 创建HTable对象
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            //get.setMaxVersions();显示所有版本
            //get.setTimeStamp();显示指定时间戳的版本
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println(" 行 键 :" +
                        Bytes.toString(result.getRow()));
                System.out.println(" 列 族 " +
                        Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列 :" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值 :" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳:" + cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取某一行数据
     */
    public void get(String tableName, String rowKey, String family, String qualifier) {
        // 创建HTable对象
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            //get.setMaxVersions();显示所有版本
            //get.setTimeStamp();显示指定时间戳的版本
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println(" 行 键 :" +
                        Bytes.toString(result.getRow()));
                System.out.println(" 列 族 " +
                        Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列 :" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值 :" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳:" + cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除行数据
     */
    public void delete(String tableName, String... rows) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List<Delete> deleteList = new ArrayList<>();
            for (String row : rows) {
                Delete delete = new Delete(Bytes.toBytes(row));
                deleteList.add(delete);
            }
            table.delete(deleteList);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

