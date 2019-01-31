package com.zzjmay.flink.sinkFunction;

import com.zzjmay.flink.domain.UserResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by zzjmay on 2019/1/31.
 */
public class MyUserPrintSinkFunction implements SinkFunction<Tuple2<Boolean,UserResult>> {


    public  void invoke(Tuple2<Boolean,UserResult> value, Context context) throws Exception  {

        System.out.println("姓名:"+value.f1.getName()+",总分:"+value.f1.getSumNum());


    }

}
