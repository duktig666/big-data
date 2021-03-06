package cn.duktig.mapreduce.writablecomparable.all;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * description:流量统计的 Bean 对象（全排序，按照总流量倒序）
 *
 * @author RenShiWei
 * Date: 2021/10/8 14:48
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class FlowBean implements WritableComparable<FlowBean> {

    /** 上行流量 */
    private long upFlow;
    /** 下行流量 */
    private long downFlow;
    /** 总流量 */
    private long sumFlow;

    public FlowBean() {
    }

    /**
     * 序列化方法 (序列化和反序列化顺序要一致)
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化方法 (序列化和反序列化顺序要一致)
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    /**
     * 重写 ToString
     */
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        //按照总流量比较,倒序排列
        if (this.sumFlow > o.sumFlow) {
            return - 1;
        } else if (this.sumFlow < o.sumFlow) {
            return 1;
        } else {
            return 0;
        }
    }


}

