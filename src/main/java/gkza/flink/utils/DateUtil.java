package gkza.flink.utils;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

/**
 * @Author: MaoMao
 * @Date: 2023/1/5 16:36
 */
public class DateUtil {

    private final static ThreadLocal<SimpleDateFormat> CDATE = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyyMMddHH")));
    private final static ThreadLocal<SimpleDateFormat> date_format = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyyMMdd")));
    private final static ThreadLocal<SimpleDateFormat> YEAR = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy")));
    public final static ThreadLocal<SimpleDateFormat> EXCEL_OUT = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")));
    private static final ThreadLocal<SimpleDateFormat> yyMMddHHmm = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy-MM-dd HH:mm")));
    private final static ThreadLocal<SimpleDateFormat> yyyyMMdd = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy-MM-dd")));
    private static final ThreadLocal<SimpleDateFormat> yyMMddHH = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyyMMddHH")));
    private static final ThreadLocal<SimpleDateFormat> obliqueDate = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy/M/d")));
    private static final ThreadLocal<SimpleDateFormat> obliqueTime = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy/M/d HH:mm:ss")));
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy-MM-dd")));
    private static final ThreadLocal<SimpleDateFormat> es_format = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")));
    private static final ThreadLocal<SimpleDateFormat> yearMonDay = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyy年MM月dd日")));
    private static final ThreadLocal<SimpleDateFormat> allDate = ThreadLocal.withInitial(()->(new SimpleDateFormat("yyyyMMddHH")));

    public static String getDateStr(Date date){
        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        return dateFormat.format(date);
    }

    public static String getAllDateStrFromCdate(int cdate) {
        SimpleDateFormat dateFormat = allDate.get();
        try {
            final String s = String.valueOf(cdate);
            Date parse = dateFormat.parse(s);
            SimpleDateFormat dateFormat1 = yyyyMMdd.get();
            return  dateFormat1.format(parse);
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }
    }

    public static Date getDateByYyyyMMdd(String dateStr) throws ParseException {
        SimpleDateFormat dateFormat = yyyyMMdd.get();
        return dateFormat.parse(dateStr);
    }

    public static Long getDateByChinese(String time){
        SimpleDateFormat dateFormat = yearMonDay.get();
        try {
            Date parse = dateFormat.parse(time);
            return parse.getTime();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static Timestamp getTtime(String time){
        SimpleDateFormat dateFormat = es_format.get();
        try {
            Date parse = dateFormat.parse(time);
            return new Timestamp(parse.getTime());
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static String getToDayDate(){
        Date date = new Date();
        SimpleDateFormat dateFormat = DATE_FORMAT.get();
        return dateFormat.format(date);
    }
    //获取当前日期
    public static String getDateNow(){
        Date now = new Date();
        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        return dateFormat.format(now);
    }


    public static String geDateNowDay(){
        Date nowDate = new Date();
        SimpleDateFormat dateFormat = yyyyMMdd.get();
        return dateFormat.format(nowDate);
    }

    public static String getDateAllNow(){
        Date nowDate = new Date();
        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        return dateFormat.format(nowDate);
    }

    public static String getDateBySecondTime(Long time) {

        if (time != null){
            SimpleDateFormat dateFormat = yyyyMMdd.get();
            if(time < 9999999999L){
                time = time *1000L;
            }
            Date date = new Date(time);
            return dateFormat.format(date);
        }else{
            return "";
        }

    }

    //获得两个日期的差值
    public static long dayDiff(String day1, String day2) throws ParseException {
        SimpleDateFormat dateFormat = yyyyMMdd.get();
        Date dt1 = dateFormat.parse(day1);
        Date dt2 = dateFormat.parse(day2);

        long between = dt2.getTime() - dt1.getTime();
        return between / 1000 / 3600 / 24;
    }

    //2023-01-06 18:01:00 => 2023010600
    public static int getDateInteger(String date) throws ParseException {

        SimpleDateFormat dateFormat = yyyyMMdd.get();
        Date dt = dateFormat.parse(date);
        String str = dateFormat.format(dt);

        String[] strings = str.split("-");
        String dateStr = "";
        for (String string : strings) {
            dateStr += string;
        }

        dateStr += "00";

        return Integer.parseInt(dateStr);
    }

    public static Integer getDateIntegerByTimestamp(Long date) throws ParseException {
        Date dt = new Date(date);
        SimpleDateFormat dateFormat = CDATE.get();
        String str = dateFormat.format(dt);
        return Integer.parseInt(str);
    }


    public static LocalDateTime getExeTime(Integer cdate){
        try {
            return LocalDateTime.parse(String.valueOf(cdate), DateTimeFormatter.ofPattern("yyyyMMddHH"));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }
    public static Long getExeTimeStamp(Integer cdate){
        try {
            return CDATE.get().parse(String.valueOf(cdate)).getTime();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }

    //获取当前时间，并转为2023010423格式
    public static int getNowInteger() {
        try {
            //String dateNow = DateUtil.getDateNow();
            long time = System.currentTimeMillis();
            time = time-86400000;
            String dateStr = getDateStr(time);
            dateStr = dateStr+"23";

            return Integer.parseInt(dateStr);
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;

    }

    public static String getDateStr(long time){
        Date date = new Date(time);
        return date_format.get().format(date);
    }

    // 毫秒级时间戳转换为字符串时间
    public static String getDateStr1(long time){
        Date date = new Date(time);
        return EXCEL_OUT.get().format(date);
    }

    public static String getDateStrNow(){
        Date date = new Date();
        return date_format.get().format(date);
    }

    public static Integer getDateFromTimestamp(Timestamp timestamp){
        SimpleDateFormat dateFormat = date_format.get();
        String format = dateFormat.format(timestamp);
        int i = Integer.parseInt(format);
        return i;

    }

    //将2023011016这个格式转换为2023-01-10格式
    public static String getDateToTimeStr(int cdate) {
        String dateStr = String.valueOf(cdate);
        //20230110
        String date = dateStr.substring(0, dateStr.length() - 2);

/*
        String format1 = "yyyyMMdd";
        String format2 = "yyyy-MM-dd";

        SimpleDateFormat inputSimple = new SimpleDateFormat(format1);
        SimpleDateFormat outputSimple = new SimpleDateFormat(format2);
        Date parse = inputSimple.parse(date);
*/
        String year = date.substring(0, 4);
        String month = date.substring(4, 6);
        String day = date.substring(6);

        return String.format("%s-%s-%s",year,month,day);
    }


    //将20240614这个格式转换为2024-06-14格式
    public static String getDateToTimeStr1(String dayStr) {

        String year = dayStr.substring(0, 4);
        String month = dayStr.substring(4, 6);
        String day = dayStr.substring(6);

        return String.format("%s-%s-%s",year,month,day);
    }

    public static Integer getYear(Long timestamp){
        Date date = new Date(timestamp);
        SimpleDateFormat dateFormat = YEAR.get();
        String format = dateFormat.format(date);
        return Integer.parseInt(format);
    }

    public static Integer getYear(Date date){
        SimpleDateFormat dateFormat = YEAR.get();
        String format = dateFormat.format(date);
        return Integer.parseInt(format);
    }

    public static Timestamp getTimestampYMD(String str) throws ParseException {
        SimpleDateFormat dateFormat = yyMMddHH.get();
        Date time = dateFormat.parse(str);
        Timestamp timestamp = new Timestamp(time.getTime());
        return timestamp;
    }

    /**
     *
     * @param date 输入可能是2023-12-31，也可能是2023
     * @return
     */
    public static Integer getYearForEnt(String date){

        if(StringUtils.isEmpty(date)){
            return null;
        }
        if(date.contains("年")){
            String[] year = date.trim().split("年");
            String s = year[0];
            return Integer.parseInt(s);
        }
        if("未公开".equals(date)){
            return null;
        }
        if (date.contains(".")){
            int index = date.indexOf(".");
            String yearStr = date.substring(0, index);
            try {
                return Integer.parseInt(yearStr);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return null;
            }
        }

        String res = "";
        if (date.contains("/")){
            int i1 = date.indexOf("/");
            res = date.substring(0,i1);

            try {
                return Integer.parseInt(res);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return null;
            }

        }

        int i = date.indexOf('-');
        if(i > 0){
             res = date.substring(0,i);
        }else {
            res = date;
        }

        try {
            return Integer.parseInt(res);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }

    /**
     *
     * @param date 输入可能是2023-12-31，也可能是2023
     * @return 返回时间戳
     */
    public static Long getDateForEnt(String date){
        if(StringUtils.isEmpty(date)){
            return null;
        }
        SimpleDateFormat dateFormat = yyyyMMdd.get();
        Long res = null;
        try {
            int i = date.indexOf('-');
            if(i > 0){
                res = dateFormat.parse(date).getTime();
            }else {
                if(StringUtils.length(date) == 4){
                    date = date+"-01-01";
                    res = dateFormat.parse(date).getTime();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return res;
    }

    public static Long getDate(Long timestamp){
        return (timestamp/86400000L*86400000L)-(8*3600000);
    }

    public static Timestamp getDate(Timestamp timestamp){
        long createTime = timestamp.getTime();
        SimpleDateFormat simpleDateFormat = yyyyMMdd.get();
        String format = simpleDateFormat.format(new Date(createTime));
        Date parse = null;
        try {
            parse = simpleDateFormat.parse(format);
        } catch (ParseException e) {
            e.printStackTrace();
            parse = new java.sql.Date(0);
        }
        return new Timestamp(parse.getTime());
    }

    public static Long getDateByStr(String date){
        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        try {
            Date parse = dateFormat.parse(date);
            return parse.getTime();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }


    /** 输入格式为 YYYY-MM-hh */
    public static Long getDateByStr1(String date){
        SimpleDateFormat dateFormat = yyyyMMdd.get();
        try {
            Date parse = dateFormat.parse(date);
            return parse.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /** 导入Hive程序检验时间格式 */
    public static String getDateStr(String str) throws ParseException {
        SimpleDateFormat simpleDateFormat = yyyyMMdd.get();
        Date parse = simpleDateFormat.parse(str);
        return str;
    }

    /** 资讯解码检验collect_time是否满足 yyyy-MM-dd HH:mm:ss */
    public static Boolean getDateSimple(String str) throws ParseException{
        SimpleDateFormat format = EXCEL_OUT.get();
        format.setLenient(false);

        try{
            Date date = format.parse(str);
            return str.equals(format.format(date));
        } catch (ParseException e){
            return false;
        }
    }

    /** */
    public static Timestamp getTimestamp(String str){
        if(StringUtils.isBlank(str) || StringUtils.equals(str,"0")){
            return null;
        }else{
            if (str.charAt(0) == '-'){
                str = "1" + str.substring(1,str.length());
                if(str.length() != 10){
                    return null;
                }
            }
        }
        if(str.contains(".")){
            int i = str.indexOf(".");
            str = str.substring(0,i);
        }
        if(str.contains("-") && str.length() == 10){
            SimpleDateFormat dateFormat = yyyyMMdd.get();
            try {
                Date parse = dateFormat.parse(str);
                return new Timestamp(parse.getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        if(str.contains("-") && str.length() > 10){
            SimpleDateFormat dateFormat = yyMMddHHmm.get();
            try {
                Date parse = dateFormat.parse(str);
                return new Timestamp(parse.getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            SimpleDateFormat dateFormat1 = yyyyMMdd.get();
            try {
                Date parse = dateFormat1.parse(str);
                return new Timestamp(parse.getTime());
            }catch (Exception e1){
                e1.printStackTrace();
            }
        }

        if(PatternUtil.isNumber(str)){
            if(str.length()==10 || str.length()==9 || str.length()==8){
                str = str+"000";
            }
            return new Timestamp(Long.parseLong(str));
        }
        return null;
    }

    /** 判断是否符合yyyy-MM-dd HH:mm:ss格式 */
    public static Boolean equalsDateFormat(String date) throws ParseException {
        SimpleDateFormat format = EXCEL_OUT.get();
        Boolean flag = false;
        try {
            format.parse(date);
            flag = true;
        } catch (ParseException e) {
            flag = false;
        }
        return flag;
    }

    /** 判断是否符合yyyy-MM-dd 格式 */
    public static Boolean equalsDateFormatYMD(String date) {
        SimpleDateFormat format = yyyyMMdd.get();
        format.setLenient(false);

        try {
            format.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /** 2022-10-18 14:24:41 -> 2022-10-18 */
    public static String getDateStrB(String str) throws ParseException {
        LocalDateTime dateTime = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    /** yyyy-MM-dd -> timestamp */
    public static Timestamp getTimeStamp(String str){
        return Timestamp.valueOf(str + " 00:00:00");
    }

    /** 2022-10-18 14:24:41 */
    public static Integer getCollectTime(String date){
        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        try {
            Date parse = dateFormat.parse(date);
            return (int)parse.getTime()/1000;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /** scala.function.UdfFunction */
    public static Integer getIntTime(String date)throws ParseException{
        if(StringUtils.isNotBlank(date)){
            SimpleDateFormat simpleDateFormat = CDATE.get();
            try {
                Date parse = simpleDateFormat.parse(date);
                long second = parse.getTime()/ 1000;
                return (int) second;
            } catch (ParseException e) {
                e.printStackTrace();

                return 0;
            }
        }
        return 0;
    }

    /** scala.function.UdfFunction.getDateCol */
    public static String getTimestampStr(String str)throws ParseException{
        if(StringUtils.isBlank(str)){
            return null;
        }
        if(str.contains(".")){
            int i = str.indexOf(".");
            str = str.substring(0,i);
        }
        if(str.contains("-")){
            return str;
        }

        if(PatternUtil.isNumber(str)){
            if(str.length()==10){
                str = str+"000";
            }
            long l = Long.parseLong(str);
            java.sql.Date date = new java.sql.Date(l);
            SimpleDateFormat simpleDateFormat = yyMMddHHmm.get();
            return simpleDateFormat.format(date);
        }
        return null;
    }

    /** yyyy.MM.dd HH:mm:ss -> yyyy-MM-dd HH:mm:ss */
    public static String getPubTime(String pub) throws ParseException {
        SimpleDateFormat dateFormat1 = EXCEL_OUT.get();
        SimpleDateFormat dateFormat2 = EXCEL_OUT.get();

        Date date = dateFormat1.parse(pub);
        return dateFormat2.format(date);
    }

    /** 处理招标pub_time */
    public static Timestamp getBidPubDate(String str){

        if (StringUtils.isNotBlank(str)){
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
            long time = 0L;
            try {
                time = dateFormat.parse(str).getTime();
            }catch (ParseException e){
                //e.printStackTrace();
                if(PatternUtil.isNumber(str)){
                    time = Long.parseLong(str + "000");

                }
            }
            return new Timestamp(time);
        }else{
            return null;
        }

    }

    /** timestamp -> dateStr */
    public static String getDateStr(Timestamp time){

        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        if (time != null){
            long time1 = time.getTime();
            return dateFormat.format(time1);
        }else{
            return "";
        }

    }

    public static String getDateStrTime(Timestamp time){

        SimpleDateFormat dateFormat = yyyyMMdd.get();
        if (time != null){
            long time1 = time.getTime();
            return dateFormat.format(time1);
        }else{
            return "";
        }

    }



    /** 获取几天后的日期 */
    public static Integer getCdateTime(String date,int next) throws ParseException {
        if(StringUtils.isNotBlank(date)){
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyyMMdd");
            Date parse = simpleDateFormat.parse(date);
            Calendar instance = Calendar.getInstance();
            instance.setTime(parse);
            instance.add(Calendar.DAY_OF_MONTH,next);
            Date time = instance.getTime();
            String format = simpleDateFormat2.format(time);
            return Integer.parseInt(format)*100;
        }
        return 0;
    }


    /** 将 yyyy-MM-dd HH:mm:ss 格式字符串 转化为 int类型 */
    public static Long dateStrToTimestamp(String str){
        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        dateFormat.setLenient(false);

        try {
            Date date = dateFormat.parse(str);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }


    /** yyyy/M/d -> yyyy-MM-dd* -> timestamp */
    public static Long dateTransform(String str) throws ParseException {
        SimpleDateFormat inFormat = obliqueDate.get();

        if (StringUtils.isNotBlank(str)){
            Date date = inFormat.parse(str);

            return date.getTime() / 1000;
        }else{
            return null;
        }


    }

    /** 2023/04/18 14:34:43 -> 秒级时间戳 */
    public static Long timeToSecondTimestamp(String str) throws ParseException {
        SimpleDateFormat dateFormat = obliqueTime.get();
        if (StringUtils.isNotBlank(str)){
            Date date = dateFormat.parse(str);

            return date.getTime() / 1000;
        }else{
            return null;
        }
    }

    /** Timestamp -> LocalDate */
    public static LocalDate timestampToLocalDate(Timestamp time){

        if (time != null){
            Instant instant = Instant.ofEpochMilli(time.getTime());
            ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
            return zonedDateTime.toLocalDate();
        }

        return null;
    }

    public static String getDatePath(Timestamp timestamp){
        SimpleDateFormat dateFormat = obliqueDate.get();
        return dateFormat.format(timestamp);
    }

    public static String getDataStr(Long time){
        SimpleDateFormat dateFormat = EXCEL_OUT.get();
        Date date = new Date(time);
        return dateFormat.format(date);
    }

    //将20240614这个格式转换为2024-06-14格式
    public static String castDateStrByYmd(String dateStr){

        if (StringUtils.equals("all", dateStr)){
            return dateStr;
        }

        if (StringUtils.isNotBlank(dateStr)){

            SimpleDateFormat dateFormat = date_format.get();

            try {
                Date parse = dateFormat.parse(dateStr);

                SimpleDateFormat dateFormat1 = yyyyMMdd.get();

                return dateFormat1.format(parse);
            } catch (Exception e) {
               e.printStackTrace();

               return null;
            }

        }else{
            return null;
        }

    }
}
