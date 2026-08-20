package gkza.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: MaoMao
 * @Date: 2026/6/23 15:39
 * @description:
 */
public class Demo1 {
    public static void main(String[] args) throws Exception {


        // TODO 1. 创建执行环境（使用 StreamExecutionEnvironment）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 关键：设置为批处理模式（Flink 1.12+ 支持）
        env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);

        // TODO 2. 读取数据：从文件中读取
        DataStream<String> dataSource = env.readTextFile("E:\\tmp\\zdhyb202503023.md");

        // TODO 3. 按行切分、转换（word,1）
        DataStream<Tuple2<String, Integer>> wordAndOne = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // TODO 3.1 按照空格切分单词
                String[] words = line.split(" ");
                // TODO 3.2 将单词转为（word,1）
                for (String word : words) {
                    if (word.length() > 0) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }
            }
        });

        // TODO 4. 按照 word 分组并聚合
        DataStream<Tuple2<String, Integer>> result = wordAndOne
                .keyBy(tuple -> tuple.f0)  // 按第一个字段（单词）分组
                .sum(1);                   // 对第二个字段求和

        // TODO 5. 输出
        result.print();

        // TODO 6. 执行作业
        env.execute("WordCount Batch Job");

    }
}
