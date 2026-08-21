package gkza.flink.demo4;

import gkza.flink.utils.GkzaConstant;
import gkza.flink.utils.PropertiesUtill;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Author: MaoMao
 * @Date: 2026/8/21 11:42
 * @description: 使用 Flink 读取 HDFS（HA 集群）上的文件
 *
 * 关键点：HDFS 是 HA 集群（hdfs://gkzacluster），FileSource 解析该路径时需要
 * dfs.nameservices、dfs.ha.namenodes 等参数。Flink 通过 HadoopUtils.getHadoopConfiguration
 * 把 Flink Configuration 中以 "flink.hadoop." 为前缀的参数剥掉前缀后写入 Hadoop Configuration。
 * 因此这里把 HA 参数以 "flink.hadoop." 前缀注入 Flink 配置，并调用 FileSystem.initialize
 * 初始化全局文件系统注册表，FileSource 即可正确解析 HA 路径。
 */
public class ReadHdfs {

    /** Flink 配置中注入 Hadoop 参数使用的前缀，HadoopUtils 会自动剥掉该前缀写入 Hadoop Configuration */
    private static final String FLINK_HADOOP_PREFIX = "flink.hadoop.";

    /** 默认读取的 HDFS 文件路径（可被配置项 hdfs.read.path 覆盖） */
    private static final String DEFAULT_HDFS_PATH =
            "hdfs://gkzacluster/special-database-json/2026-08-21/FlumeData.1787241953684.log";

    public static void main(String[] args) throws Exception {

        // 1. 读取配置文件
        Properties pro = PropertiesUtill.fromPath("FlinkDemo.properties");

        // 2. 构建 Flink 配置，将 Hadoop HA 相关参数以 flink.hadoop. 前缀注入；
        //    HadoopFsFactory 创建 HDFS 文件系统时，HadoopUtils.getHadoopConfiguration
        //    会剥掉前缀，把这些参数写入 Hadoop Configuration，从而支持 hdfs://gkzacluster/ HA 路径解析
        Configuration conf = new Configuration();
        addHadoopConfig(conf, pro, GkzaConstant.FS_DEFAULTFS);
        addHadoopConfig(conf, pro, GkzaConstant.HADOOP_DFS_NAMESERVICES);
        addHadoopConfig(conf, pro, GkzaConstant.HADOOP_DFS_HA_NAMENODES_GKZACLUSTER);
        addHadoopConfig(conf, pro, GkzaConstant.HADOOP_DFS_NAMENODE_RPC_ADDRESS_GKZACLUSTER_NN1);
        addHadoopConfig(conf, pro, GkzaConstant.HADOOP_DFS_NAMENODE_RPC_ADDRESS_GKZACLUSTER_NN2);
        addHadoopConfig(conf, pro, GkzaConstant.HADOOP_DFS_CLIENT_FAILOVER_PROXY_PROVIDER_GKZACLUSTER);

        // 3. 用该配置初始化全局 FileSystem 注册表，使 FileSource 解析 hdfs:// 路径时能拿到上面的 HA 配置
        FileSystem.initialize(conf);

        // 4. 验证是否加载成功
        System.out.println("fs.defaultFS: " + pro.getProperty(GkzaConstant.FS_DEFAULTFS));
        System.out.println("dfs.nameservices: " + pro.getProperty(GkzaConstant.HADOOP_DFS_NAMESERVICES));

        // 5. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 6. 以按行文本格式读取 HDFS 文件
        String hdfsPath = pro.getProperty("hdfs.read.path", DEFAULT_HDFS_PATH);

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(hdfsPath)
        ).build();

        DataStreamSource<String> source =
                env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "hdfs-text-source");

        source.print();

        env.execute("Read HDFS with FileSource");
    }

    /**
     * 把 properties 中的 hadoop 参数以 flink.hadoop. 前缀写入 Flink Configuration，
     * HadoopUtils 会剥掉前缀后将其作为 Hadoop 配置使用。
     *
     * @param conf Flink 配置
     * @param pro  properties 配置
     * @param key  hadoop 参数名（如 fs.defaultFS）
     */
    private static void addHadoopConfig(Configuration conf, Properties pro, String key) {
        String value = pro.getProperty(key);
        if (value != null) {
            conf.setString(FLINK_HADOOP_PREFIX + key, value);
        }
    }
}
