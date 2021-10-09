package cn.duktig.mapreduce.join.reducejoin;

import cn.duktig.mapreduce.join.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * description:join合并实战——map阶段处理 order 和 product 数据
 * <p>
 * 以连接字段 pid 作为key
 *
 * <p>
 * order文件（map阶段）：
 * 订单id  pid  数量
 * 1001   01   1
 * 1002   02   2
 * ……
 * <p>
 * product文件（map阶段）：
 * pid  产品名称
 * 01   小米
 * 02   华为
 * ……
 * <p>
 * 合并后的文件（reduce阶段）：
 * 订单id  产品名称  数量
 * 1001   小米   1
 * 1002   华为   2
 * ……
 *
 * @author RenShiWei
 * Date: 2021/10/9 9:30
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    /**
     * 初始化
     * 如果代码写在 map方法 中，没获取一行都会执行。所以写在这里
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取对应文件名称
        // 得到切片信息
        InputSplit split = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) split;
        filename = fileSplit.getPath().getName();
    }

    /**
     * 处理order和product文件中的数据
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();

        //判断是哪个文件,然后针对文件进行不同的操作
        //订单表的处理
        if (filename.contains("order")) {
            String[] split = line.split("\t");
            //封装 outK
            outK.set(split[1]);
            //封装 outV
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setName("");
            outV.setFlag("order");
        } else {                             //商品表的处理
            String[] split = line.split("\t");
            //封装 outK
            outK.set(split[0]);
            //封装 outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setName(split[1]);
            outV.setFlag("pd");
        }
        //写出 KV
        context.write(outK, outV);
    }

}

