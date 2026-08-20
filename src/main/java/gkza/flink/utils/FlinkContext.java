package gkza.flink.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: MaoMao
 * @Date: 2026/6/23 19:39
 * @description:
 */
public class FlinkContext {

    private final StreamExecutionEnvironment env;

    private final StreamTableEnvironment tableEnv;

    public FlinkContext(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) {

        this.env = env;
        this.tableEnv = tableEnv;
    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }
}
