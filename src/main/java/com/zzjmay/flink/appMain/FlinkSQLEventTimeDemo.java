package com.zzjmay.flink.appMain;

import com.zzjmay.flink.mapFunction.MyMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;


import javax.annotation.Nullable;
import java.sql.Time;

import java.text.SimpleDateFormat;

/**
 * Flink的SQL和TABLE API的demo
 * Created by zzjmay on 2019/2/12.
 */
public class FlinkSQLEventTimeDemo {

    public static void main(String[] args) throws Exception {
        int port = 9998;

        //1.创建Stream的上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);//设置并行度为1
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//设置事件时间

        //2.注册流的表上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        //3.通过socket获取流数据
        DataStream<String> text = env.socketTextStream("127.0.0.1",port);

        //4.处理获取到的流数据
        DataStream<Tuple3<String,Time,String>> clickStream = text
                //处理socket流
                .map(new MyMapFunction())
                //生成和下发水印时间
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Time, String>>() {
                    Long currentMaxTimestamp = 0L;
                    final Long maxOutOfOrderness = 1000L;// 最大允许的乱序时间

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Time, String> element, long previousElementTimestamp) {
                        long timestamp = element.f1.getTime();

                        //设置最大当前时间
                        currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp);
                        long id = Thread.currentThread().getId();
                        System.out.println("键值 :"+element.f0+",线程ID:【"+id+"】,事件事件:[ "+sdf.format(element.f1)+" ],currentMaxTimestamp:[ "+
                                sdf.format(currentMaxTimestamp)+" ],水印时间:[ "+sdf.format(getCurrentWatermark().getTimestamp())+" ]");
                        return timestamp;
                    }
                });

        //5.将流转化成动态表,并提供事件时间列
        tableEnv.registerDataStream("clicks",clickStream,"user,cTime,url,rowtime.rowtime");

        //6.写查询语句
        String sql = "select user,TUMBLE_END(rowtime,INTERVAL '60' SECOND) As endT,COUNT(url) As cnt "
                +" From clicks "
                +" GROUP BY user,TUMBLE(rowtime,INTERVAL '60' SECOND)";

        //7.执行查询操作，生成动态结果表
        Table resultTable = tableEnv.sqlQuery(sql);

        //8.先定义一个tableSink,fieldDelim = "|"分隔符
        TableSink csvSink = new CsvTableSink("/Users/zzjmay/flinkData/flinkResult2.txt","|", 1,FileSystem.WriteMode.OVERWRITE);

        String[] fieldNames = {"user","endT","cnt"};

        TypeInformation[] types = {Types.STRING(),Types.SQL_TIMESTAMP(),Types.LONG()};

        tableEnv.registerTableSink("MyTestClick",fieldNames,types,csvSink);
        //9.将结果表输出
        resultTable.insertInto("MyTestClick");

        //10.或者转化成流打印

        env.execute();

    }
}
