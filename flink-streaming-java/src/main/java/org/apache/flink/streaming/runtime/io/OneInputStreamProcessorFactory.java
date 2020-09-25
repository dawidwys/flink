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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.streaming.runtime.io.EndOfInputUtil.endInput;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class OneInputStreamProcessorFactory<IN, OUT>
	extends AbstractStreamProcessorFactory<OUT, OneInputStreamOperator<IN, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(MultiInputStreamProcessorFactory.class);

	@Override
	public StreamInputProcessor create(
			AbstractInvokable owner,
			StreamConfig config,
			Environment environment,
			OneInputStreamOperator<IN, OUT> operator,
			OperatorChain<OUT, ?> operatorChain,
			SubtaskCheckpointCoordinator checkpointCoordinator) {
		int numberOfInputs = config.getNumberOfNetworkInputs();

		WatermarkGauge inputWatermarkGauge = new WatermarkGauge();
		StreamInputProcessor inputProcessor = null;
		if (numberOfInputs > 0) {
			CheckpointedInputGate inputGate = createCheckpointedInputGate(
				owner,
				environment,
				config,
				checkpointCoordinator
			);
			EndOfInputAwareDataOutput<IN> output = createDataOutput(
				operator,
				operatorChain,
				inputWatermarkGauge
			);
			StreamTaskInput<IN> input = createTaskInput(
				config,
				environment,
				inputGate,
				output);
			inputProcessor = new StreamOneInputProcessor<>(
				input,
				output
			);
		}
		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		environment.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge::getValue);
		return inputProcessor;
	}


	private CheckpointedInputGate createCheckpointedInputGate(
			AbstractInvokable owner,
			Environment environment,
			StreamConfig configuration,
			SubtaskCheckpointCoordinator checkpointCoordinator) {
		IndexedInputGate[] inputGates = environment.getAllInputGates();

		return InputProcessorUtil.createCheckpointedInputGate(
			owner,
			configuration,
			checkpointCoordinator,
			inputGates,
			environment.getMetricGroup().getIOMetricGroup(),
			getTaskNameWithSubtaskAndId(environment));
	}

	private EndOfInputAwareDataOutput<IN> createDataOutput(
			OneInputStreamOperator<IN, OUT> operator,
			OperatorChain<OUT, ?> operatorChain,
			WatermarkGauge inputWatermarkGauge) {
		return new StreamTaskNetworkOutput<>(
			operator,
			operatorChain,
			inputWatermarkGauge,
			setupNumRecordsInCounter(operator));
	}

	private StreamTaskInput<IN> createTaskInput(
			StreamConfig configuration,
			Environment environment,
			CheckpointedInputGate inputGate,
			PushingAsyncDataInput.DataOutput<IN> output) {
		int numberOfInputChannels = inputGate.getNumberOfInputChannels();
		StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(numberOfInputChannels, output);

		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(environment.getUserCodeClassLoader()
			.asClassLoader());
		return new StreamTaskNetworkInput<>(
			inputGate,
			inSerializer,
			environment.getIOManager(),
			statusWatermarkValve,
			0);
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in one input processor.
	 */
	private static class StreamTaskNetworkOutput<IN> extends AbstractDataOutput<IN> {

		private final OneInputStreamOperator<IN, ?> operator;

		private final WatermarkGauge watermarkGauge;
		private final Counter numRecordsIn;

		private StreamTaskNetworkOutput(
			OneInputStreamOperator<IN, ?> operator,
			StreamStatusMaintainer streamStatusMaintainer,
			WatermarkGauge watermarkGauge,
			Counter numRecordsIn) {
			super(streamStatusMaintainer);

			this.operator = checkNotNull(operator);
			this.watermarkGauge = checkNotNull(watermarkGauge);
			this.numRecordsIn = checkNotNull(numRecordsIn);
		}

		@Override
		public void emitRecord(StreamRecord<IN> record) throws Exception {
			numRecordsIn.inc();
			operator.setKeyContextElement1(record);
			operator.processElement(record);
		}

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {
			watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
			operator.processWatermark(watermark);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			operator.processLatencyMarker(latencyMarker);
		}

		@Override
		public void endOutput() throws Exception {
			endInput(operator);
		}
	}
}
