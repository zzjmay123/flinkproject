package com.zzjmay.flink.test;


import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by zzjmay on 2019/2/7.
 */
public class GeneralTestDate {


    public static void main(String[] args) throws Exception {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String dateStr = "2019-02-01 10:30:20";

        System.out.println(sdf.parse(dateStr).getTime());
    }
}
