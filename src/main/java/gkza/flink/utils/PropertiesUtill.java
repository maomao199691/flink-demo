package gkza.flink.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @description:
 * @author: zhangshancheng
 **/
public class PropertiesUtill {
    //private static final Logger log= Logger.getLogger(PropertiesUtill.class);
    private static final Logger log = LoggerFactory.getLogger(PropertiesUtill.class);
    public static Properties fromPath(String path){
        Properties pro = new Properties();
        String resource = PropertiesUtill.class.getResource("").getPath();
        ClassLoader classLoader = PropertiesUtill.class.getClassLoader();
        log.info("获得路径："+resource);
        if(!resource.contains("!")){
            log.info("编程模式启动");
            InputStream in = classLoader.getResourceAsStream(path);
            Reader readers = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            try {
                pro.load(readers);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            //file:/app/gkza/spark-run-1.0-SNAPSHOT.jar!/com/ai/java/util/
            log.info("正常模式启动");
/*
            String s = resource.split("!")[0];
            int i = s.indexOf("file:");
            String substring = s.substring(i + 5);
            File file = new File(substring);
            String parent = file.getPath();
            log.info("工程的根路径为："+parent);
            File sysPath = new File(parent + File.separatorChar + path);
*/
            try {
                //Reader reader = new FileReader(sysPath);
                InputStream in = classLoader.getResourceAsStream(path);

                Reader readers = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));

                try {
                    pro.load(readers);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return pro;
    }

    public static Integer getInt(Properties p,String key){
        String trim = p.getProperty(key).trim();
        return Integer.parseInt(trim);
    }

    public static boolean isJarMode(){
        String resource = PropertiesUtill.class.getResource("").getPath();
        if(resource.contains("!")){
            return true;
        }
        return false;
    }

    public static String getTmpPath(){
        if(isJarMode()){
            return "/data/execute";
        }
        return PathUtil.getHomePath();
    }


    /**
     * @param path 读取的本地文件
     * @return 将本地文件的内容返回
     */
    public static Set<String> getLocalFileNames(String path){

        HashSet<String> result = new HashSet<>();

        ClassLoader classLoader = PropertiesUtill.class.getClassLoader();

        InputStream inputStream = classLoader.getResourceAsStream(path);

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream,StandardCharsets.UTF_8));

            String line;
            while ((line = reader.readLine()) != null){
                result.add(line.trim());
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (inputStream != null){
                    inputStream.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    public static HashMap<String, HashSet<String>> getKeyWords(Properties p){

        HashMap<String, HashSet<String>> res = new HashMap<>();
        Set<Map.Entry<Object, Object>> entries = p.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            if(StringUtils.isNotBlank(value)){
                String[] split = value.split("、");
                HashSet<String> strings = new HashSet<>();
                for (String s : split) {
                    String trim = s.trim();
                    if(StringUtils.isNotBlank(trim)){
                        strings.add(trim);
                    }
                }
                res.put(key,strings);
            }
        }

        return res;


    }

    public static void main(String[] args) {
        Properties properties = fromPath("test.properties");
    }

}
