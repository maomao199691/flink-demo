package gkza.flink.demo3;


import gkza.flink.utils.FlinkEnvUtils;
import gkza.flink.utils.PropertiesUtill;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author: MaoMao
 * @Date: 2026/6/24 13:47
 * @description:
 */
public class SocketStreamWordCount1 {
    public static void main(String[] args) throws Exception {

        Properties pro = PropertiesUtill.fromPath("FlinkDemo.properties");
        StreamExecutionEnvironment env = FlinkEnvUtils.getExecutionEnv(pro);

        // 2.读取远程机器上的文本流
        DataStreamSource<String> lineStream = env.socketTextStream("hadoop7", 7777);

        // 3.word count处理
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");

                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        // 4. 打印
        sum.print();


        env.execute();

    }
}
