package com.zzjmay.flink.appMain;

import com.zzjmay.flink.mapFunction.MyMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.sql.Time;
import java.text.SimpleDateFormat;

/**
 * Flink的SQL和TABLE API的demo
 * Created by zzjmay on 2019/2/12.
 */
public class FlinkSQLProcessTimeDemo {

    public static void main(String[] args) throws Exception {
        int port = 9998;

        //1.创建Stream的上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);//设置并行度为1

        //2.注册流的表上下文
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        //3.通过socket获取流数据
        DataStream<String> text = env.socketTextStream("127.0.0.1",port);

        //4.处理获取到的流数据
        DataStream<Tuple3<String,Time,String>> clickStream = text
                //处理socket流
                .map(new MyMapFunction());

        //5.将流转化成动态表,并提供事件时间列
        tableEnv.registerDataStream("clicks",clickStream,"user,cTime,url");

        //6.写查询语句
//        String sql = "select user,COUNT(url) As cnt "
//                +" From clicks "
//                +" GROUP BY user";
        String sql = "select * "
                +" From clicks ";

        //7.执行查询操作，生成动态结果表
        Table resultTable = tableEnv.sqlQuery(sql);


        //8.先定义一个tableSink,fieldDelim = "|"分隔符
//        TableSink csvSink = new CsvTableSink("/Users/zzjmay/flinkData/flinkResult3.txt","|", 1,FileSystem.WriteMode.OVERWRITE);
//
//        //9.将结果表输出
//        resultTable.writeToSink(csvSink);

        //10.或者转化成流打印
        DataStream<Row> tuple2DataStream = tableEnv.toAppendStream(resultTable,Row.class);

        SingleOutputStreamOperator<String> desStream = tuple2DataStream
                .map(new MapFunction<Row, String>() {
                    @Override
                    public String map(Row value) throws Exception {
                        return value.toString();
                    }
                });

        desStream.print();

        env.execute();

    }
}
