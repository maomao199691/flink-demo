package gkza.flink.demo2;

import gkza.flink.utils.FlinkEnvUtils;
import gkza.flink.utils.PropertiesUtill;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @Author: MaoMao
 * @Date: 2026/6/23 17:11
 * @description:
 */
public class ReadHiveTest {
    public static void main(String[] args) {

        Properties pro = PropertiesUtill.fromPath("FlinkDemo.properties");
        StreamTableEnvironment hiveTableEnv = FlinkEnvUtils.getHiveTableEnv(pro, true);

        String querySql = "select count(1) from database_01.patent_hebing_all limit 100";

        TableResult tableResult = hiveTableEnv.executeSql(querySql);

        tableResult.print();


    }
}
