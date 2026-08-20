package gkza.flink.utils;

/**
 * @author zsc
 * @date 2022/8/11
 * dec
 */
public class GkzaConstant {

    public final static String APP_NAME ="app.name";

    public final static String FS_DEFAULTFS ="fs.defaultFS";

    public final static String DFS_NAMENODE_SECONDARY_HTTP_ADDRESS ="dfs.namenode.secondary.http-address";

    public final static String DFS_NAMENODE_HTTP_ADDRESS ="dfs.namenode.http-address";

    public final static String MASTER ="master";

    // hive
    public final static String HIVE_METASTORE_URIS ="hive.metastore.uris";
    public final static String HIVE_DEFAULT_DATABASE ="hive.default.database";
    
    // spark
    public final static String SPARK_SQL_SHUFFLE_PARTITIONS ="spark.sql.shuffle.partitions";

    // hadoop
    public final static String SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE ="spark.sql.sources.partitionOverwriteMode";

    public final static String HADOOP_DFS_NAMESERVICES ="dfs.nameservices";

    public final static String HADOOP_DFS_HA_NAMENODES_GKZACLUSTER = "dfs.ha.namenodes.gkzacluster";

    public final static String HADOOP_DFS_NAMENODE_RPC_ADDRESS_GKZACLUSTER_NN1 = "dfs.namenode.rpc-address.gkzacluster.nn1";

    public final static String HADOOP_DFS_NAMENODE_RPC_ADDRESS_GKZACLUSTER_NN2 = "dfs.namenode.rpc-address.gkzacluster.nn2";

    public final static String HADOOP_DFS_CLIENT_FAILOVER_PROXY_PROVIDER_GKZACLUSTER = "dfs.client.failover.proxy.provider.gkzacluster";


    // hadoop
    public final static String FS_USER = "user";

    public final static String TEM_PATH = "tem.path";

    public final static String UPLOAD_PATH = "upload.path";

    public final static String FLINK_PARALLELISM = "flink.parallelism";

}
