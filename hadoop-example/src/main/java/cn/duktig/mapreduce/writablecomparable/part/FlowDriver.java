package cn.duktig.mapreduce.writablecomparable.part;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * description:
 *
 * @author RenShiWei
 * Date: 2021/10/8 15:01
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取 job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 关联本 Driver 类
        job.setJarByClass(FlowDriver.class);

        //3 关联 Mapper 和 Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4 设置 Map 端输出 KV 类型 (更换原有类型)
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5 设置程序最终输出的 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //8 ** 指定自定义分区器
        job.setPartitionerClass(PartProvincePartitioner.class);

        //9 ** 同时指定相应数量的 ReduceTask
        job.setNumReduceTasks(5);

        //6 设置程序的输入输出路径
        // writablecomparable_all 目录为之前序列化案例的文件
        FileInputFormat.setInputPaths(job, new Path("hadoop-example/src/main/resources/writablecomparable_all_input"));
        FileOutputFormat.setOutputPath(job, new Path("hadoop-example/src/main/resources" +
                "/writablecomparable_part_output"));

        //7 提交 Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

}

