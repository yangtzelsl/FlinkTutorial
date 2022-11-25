package com.yangtzelsl.chapter05;

/**
 * map transform 算子
 * 1对1
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 方式1：传入接口MapFunction的实现类
        stream.map(new UserExtractor()).print();

        // 方式2：传入匿名类，实现MapFunction
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e.user;
            }
        }).print();

        // 方式3：lambda表达式(小心泛型擦除，加returns确定类型)
        stream.map((data) -> data.user).print();

        env.execute();
    }
    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}

