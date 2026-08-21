package gkza.flink.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zsc
 * @date 2022/8/11
 * dec
 */
public class PatternUtil {

    private static final ThreadLocal<Pattern> imagePattern  = ThreadLocal.withInitial(()->(Pattern.compile("(\\@\\[([a-zA-Z0-9]+)\\]\\@)")));
    private static ThreadLocal<Pattern> number = ThreadLocal.withInitial(()->Pattern.compile("(^-?[0-9]+(\\.[0-9]+)?$)"));
    private static ThreadLocal<Pattern> getNumberStr = ThreadLocal.withInitial(()->Pattern.compile("(-?[0-9]+(\\.[0-9]+)?)"));
    private static Pattern code = Pattern.compile("^[\u3000|\u0020|\u00A0a-z0-9A-Z\u4e00-\u9fa5]*$");
    //private static final Pattern number = Pattern.compile("[^0-9]+");
    private static final String dateTimeRegex = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$";

    private static final Pattern zhPattern = Pattern.compile("[\u4e00-\u9fa5]");

    private static final Pattern have_number = Pattern.compile("([0-9]+)");


    public static boolean isNumber(String str) {
        if(StringUtils.isNotBlank(str)){
            Matcher m = number.get().matcher(str);
            return m.matches();
        }else {
            return false;
        }

    }
    public static boolean haveNumber(String str) {
        Matcher m = have_number.matcher(str);
        return m.find();
    }



    /**
     * 判断字符串中是否包含中文
     * @param str 待校验字符串
     * @return 是否为中文
     */
    public static boolean isContainsChinese(String str) {
        if (str == null) { return false; }
        Matcher m = zhPattern.matcher(str);
        return m.find();
    }

    public static boolean isCode(String str) {
        if (str == null) { return false; }
        Matcher m = code.matcher(str);
        return m.find();
    }

    public static Number getNumber(String code){
        Number resultCode = 0;
        if (StringUtils.isNotBlank(code)){
            try {
                Matcher matcher = getNumberStr.get().matcher(code);
                if(matcher.find()){
                    String group = matcher.group(0);
                    if(group.contains(".")){
                        resultCode = Double.parseDouble(group);
                    }else {
                        resultCode = Integer.parseInt(group);
                    }
                }
               /* String s = matcher.replaceAll("");
                if (StringUtils.isNotBlank(s)){
                    resultCode = Integer.parseInt(s);
                }*/
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        return resultCode;
    }

    //collect_time是否为 yyyy-MM-dd HH:mm:ss 格式
    public static boolean isDate(String timeStr){
        if (StringUtils.isBlank(timeStr)){
            return false;
        }
        if (timeStr.matches(dateTimeRegex)){
            return true;
        }else{
            return false;
        }
    }

    public static String removeImage(String text){
        Pattern pattern = imagePattern.get();
        String result = text;
        if (StringUtils.isNotEmpty(result) && result.contains("@[")){
            Matcher matcher = pattern.matcher(result);
            result = matcher.replaceAll("");
        }
        return result;
    }

}
