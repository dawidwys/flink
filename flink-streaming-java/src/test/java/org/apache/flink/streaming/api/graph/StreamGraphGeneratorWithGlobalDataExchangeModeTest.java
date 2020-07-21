/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link StreamingJobGraphGenerator} on different {@link GlobalDataExchangeMode} settings.
 */
public class StreamGraphGeneratorWithGlobalDataExchangeModeTest extends TestLogger {

	@Test
	public void testAllEdgesBlockingMode() {
		final StreamGraph streamGraph = createStreamGraph(GlobalDataExchangeMode.ALL_EDGES_BLOCKING);
		Integer sourceId = streamGraph.getSourceIDs().iterator().next();
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
		StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

		StreamNode map1Node = streamGraph.getTargetVertex(sourceOutEdge);
		StreamEdge map1OutEdge = map1Node.getOutEdges().get(0);

		StreamNode map2Node = streamGraph.getTargetVertex(map1OutEdge);
		StreamEdge map2OutEdge = map2Node.getOutEdges().get(0);

		assertThat(sourceOutEdge.getShuffleMode(), equalTo(ShuffleMode.BATCH));
		assertThat(map1OutEdge.getShuffleMode(), equalTo(ShuffleMode.BATCH));
		assertThat(map2OutEdge.getShuffleMode(), equalTo(ShuffleMode.BATCH));
	}

	@Test
	public void testAllEdgesPipelinedMode() {
		final StreamGraph streamGraph = createStreamGraph(GlobalDataExchangeMode.ALL_EDGES_PIPELINED);
		Integer sourceId = streamGraph.getSourceIDs().iterator().next();
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
		StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

		StreamNode map1Node = streamGraph.getTargetVertex(sourceOutEdge);
		StreamEdge map1OutEdge = map1Node.getOutEdges().get(0);

		StreamNode map2Node = streamGraph.getTargetVertex(map1OutEdge);
		StreamEdge map2OutEdge = map2Node.getOutEdges().get(0);

		assertThat(sourceOutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
		assertThat(map1OutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
		assertThat(map2OutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
	}

	@Test
	public void testForwardEdgesPipelinedMode() {
		final StreamGraph streamGraph = createStreamGraph(GlobalDataExchangeMode.FORWARD_EDGES_PIPELINED);
		Integer sourceId = streamGraph.getSourceIDs().iterator().next();
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
		StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

		StreamNode map1Node = streamGraph.getTargetVertex(sourceOutEdge);
		StreamEdge map1OutEdge = map1Node.getOutEdges().get(0);

		StreamNode map2Node = streamGraph.getTargetVertex(map1OutEdge);
		StreamEdge map2OutEdge = map2Node.getOutEdges().get(0);

		assertThat(sourceOutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
		assertThat(map1OutEdge.getShuffleMode(), equalTo(ShuffleMode.BATCH));
		assertThat(map2OutEdge.getShuffleMode(), equalTo(ShuffleMode.BATCH));
	}

	@Test
	public void testPointwiseEdgesPipelinedMode() {
		final StreamGraph streamGraph = createStreamGraph(GlobalDataExchangeMode.POINTWISE_EDGES_PIPELINED);
		Integer sourceId = streamGraph.getSourceIDs().iterator().next();
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
		StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

		StreamNode map1Node = streamGraph.getTargetVertex(sourceOutEdge);
		StreamEdge map1OutEdge = map1Node.getOutEdges().get(0);

		StreamNode map2Node = streamGraph.getTargetVertex(map1OutEdge);
		StreamEdge map2OutEdge = map2Node.getOutEdges().get(0);

		assertThat(sourceOutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
		assertThat(map1OutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
		assertThat(map2OutEdge.getShuffleMode(), equalTo(ShuffleMode.BATCH));
	}

	@Test
	public void testGlobalDataExchangeModeDoesNotOverrideSpecifiedShuffleMode() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<Integer> source = env.fromElements(1, 2, 3).setParallelism(1);
		final DataStream<Integer> forward = new DataStream<>(env, new PartitionTransformation<>(
			source.getTransformation(), new ForwardPartitioner<>(), ShuffleMode.PIPELINED));
		Transformation<Integer> transformation = forward.map(i -> i)
			.startNewChain()
			.setParallelism(1)
			.getTransformation();

		StreamGraph streamGraph = createStreamGraph(
			GlobalDataExchangeMode.ALL_EDGES_BLOCKING,
			Collections.singletonList(transformation));

		Integer sourceId = streamGraph.getSourceIDs().iterator().next();
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
		StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

		assertThat(sourceOutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
	}

	@Test
	public void testAutoDerivingExchangeModeForBoundedSource() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<Integer> boundedSource = env.fromSource(
			new MockSource(Boundedness.BOUNDED, 1),
			WatermarkStrategy.noWatermarks(),
			"source");

		Transformation<Integer> transformation = boundedSource.keyBy(i -> i)
			.map(i -> i)
			.getTransformation();

		StreamGraph streamGraph = createStreamGraph(null, Collections.singletonList(transformation));

		Integer sourceId = streamGraph.getSourceIDs().iterator().next();
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
		StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

		assertThat(streamGraph.getScheduleMode(), equalTo(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST));
		assertThat(sourceOutEdge.getShuffleMode(), equalTo(ShuffleMode.BATCH));
	}

	@Test
	public void testAutoDerivingExchangeModeForUnboundedSource() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final DataStream<Integer> boundedSource = env.fromSource(
			new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 1),
			WatermarkStrategy.noWatermarks(),
			"source");

		Transformation<Integer> transformation = boundedSource.keyBy(i -> i)
			.map(i -> i)
			.getTransformation();

		StreamGraph streamGraph = createStreamGraph(null, Collections.singletonList(transformation));

		Integer sourceId = streamGraph.getSourceIDs().iterator().next();
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
		StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

		assertThat(streamGraph.getScheduleMode(), equalTo(ScheduleMode.EAGER));
		assertThat(sourceOutEdge.getShuffleMode(), equalTo(ShuffleMode.PIPELINED));
	}

	/**
	 * Topology: source(parallelism=1) --(forward)--> map1(parallelism=1)
	 *           --(rescale)--> map2(parallelism=2) --(rebalance)--> sink(parallelism=2).
	 */
	private static StreamGraph createStreamGraph(@Nullable GlobalDataExchangeMode globalDataExchangeMode) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStream<Integer> source = env.fromElements(1, 2, 3).setParallelism(1);

		final DataStream<Integer> forward = new DataStream<>(env, new PartitionTransformation<>(
			source.getTransformation(), new ForwardPartitioner<>(), ShuffleMode.UNDEFINED));
		final DataStream<Integer> map1 = forward.map(i -> i).startNewChain().setParallelism(1);

		final DataStream<Integer> rescale = new DataStream<>(env, new PartitionTransformation<>(
			map1.getTransformation(), new RescalePartitioner<>(), ShuffleMode.UNDEFINED));
		final DataStream<Integer> map2 = rescale.map(i -> i).setParallelism(2);

		SinkTransformation<Integer> transformation = map2.rebalance().print().setParallelism(2).getTransformation();
		List<Transformation<?>> transformations = Collections.singletonList(transformation);

		return createStreamGraph(globalDataExchangeMode, transformations);
	}

	private static StreamGraph createStreamGraph(
			@Nullable GlobalDataExchangeMode globalDataExchangeMode,
			List<Transformation<?>> transformations) {
		StreamGraphGenerator generator = new StreamGraphGenerator(
			transformations,
			new ExecutionConfig(),
			new CheckpointConfig())
			.setStateBackend(null)
			.setChaining(true)
			.setUserArtifacts(Collections.emptyList())
			.setTimeCharacteristic(TimeCharacteristic.ProcessingTime)
			.setDefaultBufferTimeout(100L);

		if (globalDataExchangeMode != null) {
			generator = generator.setGlobalDataExchangeMode(globalDataExchangeMode);
		}

		return generator.generate();
	}
}
