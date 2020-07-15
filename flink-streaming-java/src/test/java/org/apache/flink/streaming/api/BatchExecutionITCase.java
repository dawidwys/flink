/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;

public class BatchExecutionITCase {
	@ClassRule
	public static MiniClusterResource cluster = new MiniClusterResource(
		getClusterConfiguration()
	);

	@Test
	public void singleInputTest() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.disableOperatorChaining();
		env.setParallelism(2);
		env.setMaxParallelism(2);
		env.fromElements('d', 'u', 'f', 'c', 'a', 'b')
			.assignTimestampsAndWatermarks(
				WatermarkStrategy.<Character>noWatermarks()
					.withTimestampAssigner((element, recordTimestamp) -> (long) element)
			)
			.slotSharingGroup("group1")
			.keyBy(value -> ((int) value) % 2 + 1)
			.process(new TestKeyedProcessFunction())
			.print()//.addSink(new DiscardingSink<>())
			.slotSharingGroup("group2");

		execute(env);
	}

	@Test
	public void multipleInputTest() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.disableOperatorChaining();
		env.setParallelism(2);
		env.setMaxParallelism(2);
		SingleOutputStreamOperator<Character> input1 = env.fromElements(
			'd',
			'u',
			'f',
			'c',
			'a',
			'b')
			.assignTimestampsAndWatermarks(
				WatermarkStrategy.<Character>noWatermarks()
					.withTimestampAssigner((element, recordTimestamp) -> (long) element)
			);

		SingleOutputStreamOperator<Character> input2 = env.fromElements(
			'd',
			'u',
			'f',
			'c',
			'a',
			'b')
			.assignTimestampsAndWatermarks(
				WatermarkStrategy.<Character>noWatermarks()
					.withTimestampAssigner((element, recordTimestamp) -> (long) element - 1)
			);

			input1.connect(input2)
				.keyBy(
					value -> ((int) value) % 2 + 1,
					value -> ((int) value) % 2 + 1,
					TypeInformation.of(Integer.class))
			.process(new TestKeyedCoProcessFunction())
			.print()//.addSink(new DiscardingSink<>())
			.slotSharingGroup("group2");

		execute(env);
	}

	private void execute(StreamExecutionEnvironment env) throws JobExecutionException, InterruptedException {
		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
		cluster.getMiniCluster().executeJobBlocking(streamGraph.getJobGraph());
	}

	private static class TestKeyedProcessFunction extends KeyedProcessFunction<Integer, Character, String> {
		@Override
		public void processElement(Character value, Context ctx, Collector<String> out) throws Exception {
			out.collect("" + value);
		}
	}

	private static class TestKeyedCoProcessFunction extends KeyedCoProcessFunction<Integer, Character, Character, String> {
		@Override
		public void processElement1(
				Character value,
				Context ctx,
				Collector<String> out) throws Exception {
			out.collect("1>" + value);
		}

		@Override
		public void processElement2(
				Character value,
				Context ctx,
				Collector<String> out) throws Exception {
			out.collect("2>" + value);
		}
	}

	private static MiniClusterResourceConfiguration getClusterConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setString(RestOptions.BIND_PORT, "0");
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 100L);
		return new MiniClusterResourceConfiguration.Builder()
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(8)
			.setConfiguration(configuration)
			.build();
	}
}
