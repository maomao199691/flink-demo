package gkza.flink.utils;


import org.apache.commons.lang3.StringUtils;

import java.net.Inet4Address;
import java.net.InetAddress;

/**
 * @author zsc
 * date  2022/5/13
 * dec
 */
public class PathUtil {
    /**
    jar里跑得到路径：file:/app/gkza/spark-run-1.0-SNAPSHOT.jar!/com/ai/scala/
     */
    public static String getHomePath(){
        String path;
        path = PathUtil.class.getResource("").getPath();
        //System.out.println("原始路径："+path);
        if(path.startsWith("file:")){
            path = path.substring(5);
        }
        if(path.contains("!")){
            String[] split = path.split("!");
            path = split[0];
        }
        int i = path.lastIndexOf("/");
        path = path.substring(0,i);

        if(path.contains("target") && path.contains("classes")){
            int i2 = path.lastIndexOf("classes");
            if(i2 != -1){
                path = path.substring(0,i2);
            }
        }
        if(path.endsWith("/")){
            int length = path.length();
            path = path.substring(0,length-1);
        }
        return path;
    }
    public static String getJarPath(){
        return PathUtil.class.getResource("").getPath();
    }

    // 获取本机ip地址
    public static String getLocalIpAddr(){
        InetAddress ip;      //用于获取IP(因为是静态的所以不需要使用new来实例化
        String IP = null;    //用于返回IP
        try {
            ip = Inet4Address.getLocalHost();    //获取IP
            IP = ip.getHostAddress();        	//以字符串形式存储IP
        } catch (Exception e) {
            e.printStackTrace();
        }

        return IP;
    }


    /**
     * 修改
     * https://www.ndcpa.gov.cn//jbkzzx/c100081/1788432040413999104/rf269n24.pdf
     * 改成：
     * https://www.ndcpa.gov.cn/jbkzzx/c100081/1788432040413999104/rf269n24.pdf
     *
     * 修改
     *  http://www.ndcpa.gov.cn//jbkzzx/c100081/1788432040413999104/rf269n24.pdf
     * 改成：
     *  http://www.ndcpa.gov.cn/jbkzzx/c100081/1788432040413999104/rf269n24.pdf
     * @param url
     * @return
     */
    public static String fixUrl(String url){
        if(StringUtils.isNotBlank(url)){
            String protocol = "";
            String body = "";
            if(url.startsWith("https://")){
                protocol = "https://";
                body = url.substring(8);

            }else if(url.startsWith("http://")){
                protocol = "http://";
                body = url.substring(7);
            }else {
                body = url;
            }
            if(StringUtils.isNotBlank(body) && body.contains("//")){
                body = body.replaceAll("//","/");
            }
            url = protocol+body;

        }

        return url;
    }


    public static boolean isWin(){
        String s = System.getProperty("os.name").toLowerCase();
        return s.contains("windows");
    }
}
