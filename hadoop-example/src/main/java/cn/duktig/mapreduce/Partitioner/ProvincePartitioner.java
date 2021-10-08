package cn.duktig.mapreduce.Partitioner;

import cn.duktig.mapreduce.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * description: 自定义分区类
 *
 * @author RenShiWei
 * Date: 2021/10/8 17:22
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 分区逻辑
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //获取手机号前三位 prePhone
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        //定义一个分区号变量 partition,根据 prePhone 设置分区号
        int partition;

        switch (prePhone) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
                break;
        }

        //最后返回分区号 partition
        return partition;
    }

}

