package cn.duktig.mapreduce.OutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * description: 读写类
 *
 * @author RenShiWei
 * Date: 2021/10/8 21:22
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream duktigOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            //获取文件系统对象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //用文件系统对象创建两个输出流对应不同的目录
            duktigOut = fs.create(new Path("hadoop-example/src/main/resources/OutputFormat_output/duktig.log"));
            otherOut = fs.create(new Path("hadoop-example/src/main/resources/OutputFormat_output/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        //根据一行的 log 数据是否包含 duktig,判断两条输出流输出的内容
        if (log.contains("duktig")) {
            duktigOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //关流
        IOUtils.closeStream(duktigOut);
        IOUtils.closeStream(otherOut);
    }
}

