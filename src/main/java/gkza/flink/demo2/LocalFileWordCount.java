package gkza.flink.demo2;

import gkza.flink.utils.FlinkEnvUtils;
import gkza.flink.utils.PropertiesUtill;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author: MaoMao
 * @Date: 2026/6/24 13:52
 * @description:
 */
public class LocalFileWordCount {
    public static void main(String[] args) throws Exception {

        Properties pro = PropertiesUtill.fromPath("FlinkDemo.properties");
        StreamExecutionEnvironment env = FlinkEnvUtils.getExecutionEnv(pro, true);

        String filePath = pro.getProperty("local.file");

        DataStreamSource<String> streamSource = env.readTextFile(filePath);

        DataStream<Tuple2<String, Integer>> wordAndOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) {
                // 按照空格切分单词
                String[] words = line.split(" ");
                // 将单词转为（word,1）
                for (String word : words) {
                    if (word.length() > 0) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }
            }
        });

        //  按照 word 分组并聚合
        DataStream<Tuple2<String, Integer>> result = wordAndOne
                .keyBy(tuple -> tuple.f0)  // 按第一个字段（单词）分组
                .sum(1);                   // 对第二个字段求和

        // 输出
        result.print();

        // 执行作业
        env.execute("WordCount Batch Job");

    }
}
