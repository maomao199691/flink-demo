package gkza.flink.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @Author: MaoMao
 * @Date: 2026/6/23 18:51
 * @description: Flink环境获取工具，当前支持批数据读取，支持hive数据读取
 */
public class FlinkEnvUtils {

    private static final Logger log = LoggerFactory.getLogger(FlinkEnvUtils.class);

    /**
     * @param prop
     * @return 默认流式处理
     */
    public static StreamExecutionEnvironment getExecutionEnv(Properties prop){
        return getExecutionEnv(prop, false);
    }


    /**
     * @return StreamExecutionEnvironment
     * @param batchMode 是否批处理
     */
    public static StreamExecutionEnvironment getExecutionEnv(Properties prop, boolean batchMode) {

        StreamExecutionEnvironment env = createStreamExecutionEnvironment(prop, batchMode);

        log.info("Flink环境创建完成，返回StreamExecutionEnvironment");
        return env;
    }

    /**
     * @return 支持Hive的StreamTableEnvironment
     * @param batchMode 是否批处理
     */
    public static StreamTableEnvironment getHiveTableEnv(Properties prop, boolean batchMode) {

        StreamExecutionEnvironment env = createStreamExecutionEnvironment(prop, batchMode);

        EnvironmentSettings settings =
                batchMode ? EnvironmentSettings
                           .newInstance()
                           .inBatchMode()
                           .build()
                        : EnvironmentSettings
                           .newInstance()
                           .inStreamingMode()
                           .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        log.info("Flink Table环境创建完成");

        registerHive(tableEnv,prop);

        log.info("Flink环境创建完成，返回StreamTableEnvironment");
        return tableEnv;
    }

    /**
     * @return 默认流式处理的Hive StreamTableEnvironment
     */
    public static StreamTableEnvironment getHiveTableEnv(Properties prop) {
        return getHiveTableEnv(prop, false);
    }

    private static StreamExecutionEnvironment createStreamExecutionEnvironment(Properties prop, boolean batchMode) {

        log.info("开始创建Flink环境，batchMode={}", batchMode);

        int parallelism = Integer.parseInt(prop.getProperty(GkzaConstant.FLINK_PARALLELISM,"4"));

        StreamExecutionEnvironment env;

        if (PathUtil.isWin()){
            Configuration conf = new Configuration();

            conf.setString(
                    "taskmanager.memory.network.min",
                    prop.getProperty("taskmanager.memory.network.min", "1024mb"));

            conf.setString(
                    "taskmanager.memory.network.max",
                    prop.getProperty("taskmanager.memory.network.max", "1024mb"));

            conf.setString(
                    "taskmanager.memory.network.fraction",
                    prop.getProperty("taskmanager.memory.network.fraction", "0.2"));

            env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
            log.info("创建本地Flink环境，network.min={}, network.max={}, network.fraction={}",
                    conf.getString("taskmanager.memory.network.min", "1024mb"),
                    conf.getString("taskmanager.memory.network.max", "1024mb"),
                    conf.getString("taskmanager.memory.network.fraction", "0.2"));
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        if (batchMode) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            log.info("Flink运行模式设置为批处理模式");
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            log.info("Flink运行模式设置为流处理模式");
        }

        env.setParallelism(parallelism);
        log.info("Flink并行度设置为{}", parallelism);

        return env;
    }

    private static void registerHive(StreamTableEnvironment tableEnv, Properties prop) {

        log.info("开始注册Hive Catalog");
        String metastoreUris = prop.getProperty(GkzaConstant.HIVE_METASTORE_URIS);

        if (StringUtils.isBlank(metastoreUris)){
            log.error("Hive metastore uris参数为空，key={}", GkzaConstant.HIVE_METASTORE_URIS);
            System.exit(1);
        }


        String defaultDb = prop.getProperty(GkzaConstant.HIVE_DEFAULT_DATABASE, "default");

        String hiveVersion = prop.getProperty("hive.version", "3.1.2");
        log.info("Hive Catalog配置：defaultDb={}, hiveVersion={}, metastoreUris={}", defaultDb, hiveVersion, metastoreUris);

        HiveConf hiveConf = new HiveConf();

        /**
         * Hive配置
         */
        hiveConf.set(GkzaConstant.HIVE_METASTORE_URIS, metastoreUris);

        /**
         * HDFS配置
         */
        copyProperty(prop, hiveConf, GkzaConstant.FS_DEFAULTFS);

        copyProperty(prop, hiveConf, GkzaConstant.HADOOP_DFS_NAMESERVICES);

        copyProperty(prop, hiveConf, GkzaConstant.HADOOP_DFS_HA_NAMENODES_GKZACLUSTER);

        copyProperty(prop, hiveConf, GkzaConstant.HADOOP_DFS_NAMENODE_RPC_ADDRESS_GKZACLUSTER_NN1);

        copyProperty(prop, hiveConf, GkzaConstant.HADOOP_DFS_NAMENODE_RPC_ADDRESS_GKZACLUSTER_NN2);

        copyProperty(prop, hiveConf, GkzaConstant.HADOOP_DFS_CLIENT_FAILOVER_PROXY_PROVIDER_GKZACLUSTER);

        HiveCatalog hiveCatalog = new org.apache.flink.table.catalog.hive.HiveCatalog("hive", defaultDb, hiveConf, hiveVersion);

        tableEnv.registerCatalog("hive", hiveCatalog);

        tableEnv.useCatalog("hive");

        log.info("Hive Catalog注册完成，并切换到hive catalog");
    }

    private static void copyProperty(Properties prop, org.apache.hadoop.conf.Configuration conf, String key) {

        String value = prop.getProperty(key);

        if (value == null) {
            value = prop.getProperty("spark.hadoop." + key);
        }

        if (value != null) {
            conf.set(key, value);
            log.debug("设置Hadoop配置：{}={}", key, value);
        } else {
            log.debug("跳过Hadoop配置，未找到参数：{}", key);
        }
    }

}
