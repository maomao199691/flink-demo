package gkza.flink.demo4;

import gkza.flink.utils.FlinkEnvUtils;
import gkza.flink.utils.PropertiesUtill;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.fs.PathFilter;

import java.util.Properties;
import java.util.function.Predicate;

/**
 * @Author: MaoMao
 * @Date: 2026/8/21 16:05
 * @description: 读取hdfs上的文件，排除掉当天的路径下的文件
 */
public class ReadHdfsNotToday {
    public static void main(String[] args) throws Exception {

        Properties pro = PropertiesUtill.fromPath("FlinkDemo.properties");

        StreamExecutionEnvironment env = FlinkEnvUtils.getExecutionEnv(pro);

        String hdfsPath = "hdfs://gkzacluster/special-database-json/2026-08-21";

        // 1. 自定义文件过滤器
        Predicate<Path> logFileFilter = new MyPathFilter();

        // 2. 创建 FileEnumerator.Provider
        FileEnumerator.Provider provider =
                new FileEnumerator.Provider() {

                    @Override
                    public FileEnumerator create() {
                        return new NonSplittingRecursiveEnumerator(
                                logFileFilter
                        );
                    }
                };

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(hdfsPath)
                ).setFileEnumerator(provider)
                .build();

        DataStreamSource<String> source =
                env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "hdfs-text-source");

        source.print();

        env.execute("Read HDFS with FileSource");
    }
}
