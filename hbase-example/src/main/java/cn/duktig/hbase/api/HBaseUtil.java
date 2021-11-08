package cn.duktig.hbase.api;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * description:
 *
 * @author RenShiWei
 * Date: 2021/11/8 17:54
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class HBaseUtil {

    /**
     * 创建命名空间
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        // 1.获取连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.conf);
        // 2.获取Admin对象
        Admin admin = connection.getAdmin();
        // 3.构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        // 4.创建命名空间
        admin.createNamespace(namespaceDescriptor);
        // 5.关闭资源
        admin.close();
        connection.close();
    }

    /**
     * @return 表是否存在
     */
    public static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.conf);
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return exists;
    }

    /**
     * 创建表
     *
     * @param tableName      表明
     * @param versions       版本
     * @param columnFamilies 列族列表
     */
    public static void createTable(String tableName, int versions, String... columnFamilies) throws IOException {
        // 判断是否传入列族
        if (columnFamilies.length <= 0) {
            throw new RuntimeException("请设置列族信息");
        }

        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            return;
        }
        // 获取连接对象
        Connection connection = ConnectionFactory.createConnection(Constants.conf);
        // 获取Admin对象
        Admin admin = connection.getAdmin();

        //创建表属性对象,表名需要转字节
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //创建多个列族
        for (String cf : columnFamilies) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            descriptor.addFamily(new HColumnDescriptor(cf));
        }
        //根据对表的配置，创建表
        admin.createTable(descriptor);
        System.out.println("表" + tableName + "创建成功！");

        admin.close();
        connection.close();
    }

}

