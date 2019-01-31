package com.zzjmay.flink.appMain;


import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;


/**
 * 分析wiki的操作
 * Created by zzjmay on 2019/1/29.
 */
public class WikipediaAnalysis {


    public static void main(String[] args) throws Exception {

        //创建一个Streaming程序运行的上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加一个source--数据来源部分
        final DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

        //创建一个有key的流
        KeyedStream<WikipediaEditEvent,String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                return wikipediaEditEvent.getUser();
            }
        });

        //进行数据处理
        DataStream<Tuple2<String,Long>> result = keyedEdits.timeWindow(Time.seconds(5))
                .fold(new Tuple2<String,Long>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String,Long>>() {

                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });

        //sink操作
        result.print();
        env.execute("测试FlinkDemo2");


    }

}
