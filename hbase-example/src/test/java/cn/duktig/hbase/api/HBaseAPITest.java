package cn.duktig.hbase.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseAPITest {

    private HBaseAPI hBaseAPI = null;

    @Before
    public void init() {
        hBaseAPI = new HBaseAPI();
        hBaseAPI.init();
    }

    @After
    public void close() {
        hBaseAPI.close();
    }

    @Test
    public void isTableExist() {
        boolean exist = hBaseAPI.isTableExist("test");
        System.out.println(exist);
    }

    @Test
    public void createTable() {
        hBaseAPI.createTable("user", "student");
    }

    @Test
    public void dropTable() {
        hBaseAPI.dropTable("user");
    }

    @Test
    public void addRowData() {
        hBaseAPI.put("user", "1", "student", "name", "小二");
        hBaseAPI.put("user", "2", "student", "name", "张三");
        hBaseAPI.put("user", "3", "student", "name", "李四");
        hBaseAPI.put("user", "4", "student", "name", "王五");
    }

    @Test
    public void scan() {
        hBaseAPI.scan("user");
    }

    @Test
    public void get() {
        hBaseAPI.get("user", "2");
        hBaseAPI.get("user", "3", "student", "name");
    }

    @Test
    public void delete() {
        hBaseAPI.delete("user", "2", "4");
    }


}
