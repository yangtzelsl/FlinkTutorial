package com.yangtzelsl.chapter06;

/**
 * 测试窗口聚合函数，统计PV UV
 */

import com.yangtzelsl.chapter05.ClickSource;
import com.yangtzelsl.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("data");

        // 所有数据设置相同的key，发送到同一个分区统计PV和UV，再相除
        // 即平均每个用户活跃度 PV/UV
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();


        env.execute();
    }

    public static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {
        /**
         * 创建累加器，生命周期方法，只调用一次
         * @return
         */
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            // 创建累加器，给定初始值
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        /**
         * 每来一个数据，累加一次
         * @param value
         * @param accumulator
         * @return
         */
        @Override
        public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
            // 属于本窗口的数据来一条累加一次，并返回累加器
            accumulator.f0.add(value.user);
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }

        /**
         * 返回结果，触发一次计算就调用一次
         * @param accumulator
         * @return
         */
        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            // 窗口闭合时，增量聚合结束，将计算结果发送到下游
            return (double) accumulator.f1 / accumulator.f0.size();
        }

        /**
         * 合并两个累加器，针对会话窗口使用
         * @param a
         * @param b
         * @return
         */
        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }
}

