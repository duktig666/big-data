package cn.duktig.mapreduce.join.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * description:join合并实战——map合并 order 和 product 数据
 *
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
 * Date: 2021/10/9 11:29
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();

    /**
     * 初始化——任务开始前将 pd 数据缓存进 pdMap
     * 如果代码写在 map方法 中，没获取一行都会执行。所以写在这里
     */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {

        //通过缓存文件得到小表数据 pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统对象,并打开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //通过包装流转换为 reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割一行： 01 小米
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }

        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //读取大表数据： 1001 01 1
        String[] fields = value.toString().split("\t");

        //通过大表每行数据的 pid,去 pdMap 里面取出 pname
        String name = pdMap.get(fields[1]);

        //将大表每行数据的 pid 替换为 name
        outK.set(fields[0] + "\t" + name + "\t" + fields[2]);

        //写出
        context.write(outK, NullWritable.get());
    }

}

