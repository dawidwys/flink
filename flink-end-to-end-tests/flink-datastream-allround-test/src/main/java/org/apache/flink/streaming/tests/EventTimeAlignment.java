/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** A simple job that's inducing event time mis-alignment. */
public class EventTimeAlignment {
    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 10));

        DataStream<Long> eventStream =
                env.fromSource(
                                new NumberSequenceSource(0, Long.MAX_VALUE),
                                WatermarkStrategy.<Long>forMonotonousTimestamps()
                                        .withTimestampAssigner(new LongTimestampAssigner()),
                                "NumberSequenceSource")
                        .map(
                                new RichMapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                                            Thread.sleep(1);
                                        }
                                        return 1L;
                                    }
                                });

        eventStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(
                        new ProcessAllWindowFunction<Long, Long, TimeWindow>() {
                            @Override
                            public void process(
                                    Context context, Iterable<Long> elements, Collector<Long> out)
                                    throws Exception {
                                long count = 0;
                                for (Long ignored : elements) {
                                    count++;
                                }
                                out.collect(count);
                            }
                        })
                .print();
        env.execute("Even time alignment test job");
    }

    private static class LongTimestampAssigner implements SerializableTimestampAssigner<Long> {
        private long counter = 0;

        @Override
        public long extractTimestamp(Long record, long x) {
            return counter++;
        }
    }
}
