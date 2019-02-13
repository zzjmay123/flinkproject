package com.zzjmay.flink.mapFunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.sql.Time;

/**
 * Created by zzjmay on 2019/2/12.
 */
public class MyMapFunction implements MapFunction<String, Tuple3<String, Time, String>> {
    @Override
    public Tuple3<String, Time, String> map(String value) throws Exception {
        String[] array = value.split(",");
        //处理时间函数
        Time time = new Time(System.currentTimeMillis());
        return new Tuple3<>(array[0],time,array[1]);
    }
}
