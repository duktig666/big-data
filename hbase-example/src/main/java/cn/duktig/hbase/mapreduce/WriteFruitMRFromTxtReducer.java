package cn.duktig.hbase.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * description:
 *
 * @author RenShiWei
 * Date: 2021/11/8 10:19
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class WriteFruitMRFromTxtReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException,
            InterruptedException {
        //读出来的每一行数据写入到fruit_hdfs表中
        for (Put put : values) {
            context.write(NullWritable.get(), put);
        }
    }
}

