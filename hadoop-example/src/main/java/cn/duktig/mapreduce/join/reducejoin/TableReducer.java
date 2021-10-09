package cn.duktig.mapreduce.join.reducejoin;

import cn.duktig.mapreduce.join.TableBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * description:join实例——reduce阶段，合并 order 和 product 数据
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
 * <p>
 * 到达reduce的数据（两个文件的数据都会 以pid为key 到达reduce阶段）：
 * 1001   01   1
 * 1002   02   2
 * 01   小米
 * 02   华为
 * ……
 *
 * @author RenShiWei
 * Date: 2021/10/9 10:31
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException,
            InterruptedException {
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        // 相同的key，循环遍历value
        for (TableBean value : values) {
            //判断数据来自哪个表
            if ("order".equals(value.getFlag())) {   //订单表
                //创建一个临时 TableBean 对象接收 value
                TableBean tmpOrderBean = new TableBean();

                /*
                    这里不能使用 orderBeans.add(value);
                    因为 Iterable<TableBean> values 不是Java中的迭代器，是Hadoop提供的，添加的是地址而且会覆盖
                    造成的结果是：最终只会添加一个对象
                    也可以考虑使用 高效一点的 BeanUtils.copy
                 */
                tmpOrderBean.setAmount(value.getAmount());
                tmpOrderBean.setFlag(value.getFlag());
                tmpOrderBean.setId(value.getId());

                //将临时 TableBean 对象添加到集合 orderBeans
                orderBeans.add(tmpOrderBean);
            } else {
                //商品表
                pdBean.setName(value.getName());
                pdBean.setFlag(value.getFlag());
            }
        }

        //遍历集合 orderBeans,替换掉每个 orderBean 的 pid 为 name,然后写出
        for (TableBean orderBean : orderBeans) {
            orderBean.setName(pdBean.getName());
            //写出修改后的 orderBean 对象
            context.write(orderBean, NullWritable.get());
        }
    }
}

