package com.zzjmay.flink.appMain;

import com.zzjmay.flink.domain.User;
import com.zzjmay.flink.domain.UserResult;
import com.zzjmay.flink.sinkFunction.MyUserPrintSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * StreamSqlDemo
 * 实现统计USER表中的按学生纬度的分数加总
 * Created by zzjmay on 2019/1/31.
 */
public class StreamSQLDemo {


    public static void main(String[] args) throws Exception {

        //1. 创建Stream的上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 获取StreamTale的上下文
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(env);

        //3.模拟流的注入,这里的流可以是mq，mysql等动态数据源
        DataStream<User> userAStream = env.fromCollection(Arrays.asList(
                new User(1L,"zhouzhenjiang3","java",80),
                new User(1L,"zhouzhenjiang3","C",79),
                new User(2L,"jony","python",99),
                new User(2L,"jony","java",100),
                new User(3L,"zzjmay","C",100),
                new User(3L,"zzjmay","java",60)));

        //4.将对应的流转化成表
//        Table userA = tableEnvironment.fromDataStream(userAStream,"userId,name,project,score");这种方式源码看是自动生成唯一的表名
        //这种方式自定义生成表名UserA
        tableEnvironment.registerDataStream("UserA",userAStream,"userId,name,project,score");

        //5.输出对应的结果
        String sql = "select name,sum(score) as sumNum  from UserA group by name";
        Table result = tableEnvironment.sqlQuery(sql);

        //6.将表转化成数据流并sink
        tableEnvironment.toRetractStream(result, UserResult.class).addSink(new MyUserPrintSinkFunction());

        env.execute("Stream Sql");
    }
}
