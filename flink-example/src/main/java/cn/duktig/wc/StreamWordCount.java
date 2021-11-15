package cn.duktig.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 *
 * @author RenShiWei
 * Date: 2021/11/15 9:37
 * blog: https://duktig.cn/
 * github知识库: https://github.com/duktig666/knowledge
 **/
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度（多少个线程同时执行，默认为电脑的核心数）
        env.setParallelism(2);

        // 从文件中读取数据 （相对路径找不到）
        String inputPath = "D:\\IDE\\code\\big-data\\flink-example\\src\\main\\resources\\hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        //用 ParameterTool 工具从程序启动参数中提取配置项
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
//        // 从socket文本流中读取数据  在linux下，使用命令 `nc -lk 7777 （端口号）` 监听端口开启文本流
//        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultDataStream = inputDataStream.flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);

        resultDataStream.print();

        // 执行任务
        env.execute();
    }

}

