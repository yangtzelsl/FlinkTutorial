package com.yangtzelsl.chapter05;

/**
 * flap map
 * 1对1 或者 1对多 先打散，再转换
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.flatMap(new MyFlatMap()).print("1");

        stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                if (value.user.equals("Mary")) {
                    // 收集数据
                    out.collect(value.user);
                } else if (value.user.equals("Bob")) {
                    // 针对Bob的数据，打散，1条变3条
                    out.collect(value.user);
                    out.collect(value.url);
                    out.collect(value.timestamp.toString());
                }
            }
        }).print("2");


        stream.flatMap((Event value, Collector<String> out) -> {
                    if (value.user.equals("Mary")) {
                        // 收集数据
                        out.collect(value.user);
                    } else if (value.user.equals("Bob")) {
                        out.collect(value.user);
                        out.collect(value.url);
                    }
                })
                // Caused by: org.apache.flink.api.common.functions.InvalidTypesException: The generic type parameters of 'Collector' are missing.
                .returns(new TypeHint<String>() {})
                .print("3");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")) {
                // 收集数据
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }
}

