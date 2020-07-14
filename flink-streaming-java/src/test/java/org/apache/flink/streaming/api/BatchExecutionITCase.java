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
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.Collector;

import org.junit.Test;

public class BatchExecutionITCase {
	@Test
	public void testBlockingShuffleScheduling() throws Exception {
		final JobGraph jobGraph = getJobGraphUnderTest();
		runHandleJobsWhenNotEnoughSlots(jobGraph);
	}

	private JobGraph getJobGraphUnderTest() {
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
			.process(new TestStatefulKeyedProcessFunction())
			.print()//.addSink(new DiscardingSink<>())
			.slotSharingGroup("group2");
		final StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
		final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
//		for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
//			for (IntermediateDataSet interm : jobVertex.getProducedDataSets()) {
//				assertEquals(ResultPartitionType.BLOCKING, interm.getResultType());
//			}
//		}
		return jobGraph;
	}

	private static class TestStatefulKeyedProcessFunction extends KeyedProcessFunction<Integer, Character, String> {
		private ValueState<Character> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("test-state", CharSerializer.INSTANCE));
		}

		@Override
		public void processElement(Character value, Context ctx, Collector<String> out) throws Exception {
//			if (value % 2 == 0) {
//				Character stored = state.value();
//				state.update(value);
//				out.collect("" + stored + value);
//			} else {
				out.collect("" + value);
//			}
		}
	}

	private void runHandleJobsWhenNotEnoughSlots(final JobGraph jobGraph) throws Exception {
		final Configuration configuration = getDefaultConfiguration();
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 100L);
		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(1)
			.setConfiguration(configuration)
			.build();
		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();
			miniCluster.executeJobBlocking(jobGraph);
		}
	}

	private Configuration getDefaultConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setString(RestOptions.BIND_PORT, "0");
		return configuration;
	}
}
