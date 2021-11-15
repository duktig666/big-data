package cn.duktig.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * description:词频统计
 *
 * @author RenShiWei
 * Date: 2021/11/15 9:25
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据 （相对路径找不到）
        String inputPath = "D:\\IDE\\code\\big-data\\flink-example\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 空格分词打散(word,1)之后，对单词进行 groupBy 分组（按照第一个位置0进行分组），然后用 sum进行聚合（第二个位置进行求和）
        DataSet<Tuple2<String, Integer>> wordCountDataSet =
                inputDataSet.flatMap(new MyFlatMapper())
                        .groupBy(0)
                        .sum(1);
        // 打印输出
        wordCountDataSet.print();
    }


}

