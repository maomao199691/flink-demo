package gkza.flink.demo1;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @Author: MaoMao
 * @Date: 2026/6/23 16:31
 * @description:
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 设置 WebUI 绑定的端口，默认为 8081
        conf.setString(RestOptions.BIND_PORT, "8081");
        // 使用这个配置来创建带有 WebUI 的本地环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                .addSink(new AlertSink())
                .name("send-alerts");

//        alerts.print();

        env.execute("Fraud Detection");

    }
}
