package com.zzjmay.flink.appMain;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 水印测试demo
 * Created by zzjmay on 2019/2/7.
 */
public class WaterMarketDemo {

    public static void main(String[] args) throws Exception {
        //定义socket端口
        int port = 9999;

        //1. 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置使用的事件事件，默认使用的是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.设置并行度，默认并行度是当前CPU的核
        env.setParallelism(2);
        //4.模拟输入数据源
        DataStream<String> text = env.socketTextStream("127.0.0.1",port);
        //5.数据处理分割
        DataStream<Tuple2<String,Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //6.设置时间戳和生成水印
        DataStream<Tuple2<String,Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 3000L;// 最大允许的乱序时间是3s

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

             /**
             * 下发水印
             * @return
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            /**
             * 用于生成时间戳
             * @param element
             * @param previousElementTimestamp
             * @return
             */
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;

                //设置最大当前时间
                currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp);
                long id = Thread.currentThread().getId();
                System.out.println("作者：zzjmay 键值 :"+element.f0+",线程ID:【"+id+"】,事件事件:[ "+sdf.format(element.f1)+" ],currentMaxTimestamp:[ "+
                        sdf.format(currentMaxTimestamp)+" ],水印时间:[ "+sdf.format(getCurrentWatermark().getTimestamp())+" ]");


             return timestamp;
            }
        });


        DataStream<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))//按照消息的EventTime分配窗口，和调用TimeWindow效果一样
//                .allowedLateness(Time.seconds(2))//设置允许延迟的2s的数据再次触发策略
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next.f1);
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = "\n 作者：zzjmay 键值 : "+ key + "\n              触发窗内数据个数 : " + arrarList.size() + "\n              触发窗起始数据： " + sdf.format(arrarList.get(0)) + "\n              触发窗最后（可能是延时）数据：" + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "\n              实际窗起始和结束时间： " + sdf.format(window.getStart()) + "《----》" + sdf.format(window.getEnd()) + " \n \n ";

                        out.collect(result);
                    }


                });
        //测试-把结果打印到控制台即可
        window.print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("eventtime-watermark");



    }
}
