package cn.duktig.mapreduce.writablecomparable.part;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * description:
 *
 * @author RenShiWei
 * Date: 2021/10/8 14:58
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        //遍历 values 集合,循环写出,避免总流量相同的情况
        for (Text value : values) {
            //调换 KV 位置,反向写出
            context.write(value, key);
        }
    }

}

