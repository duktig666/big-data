package cn.duktig.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 自定义类，实现 FlatMapFunction 接口
 */
public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = value.split(" ");
        for (String word : words) {
            out.collect(new Tuple2<>(word, 1));
        }
    }

}
