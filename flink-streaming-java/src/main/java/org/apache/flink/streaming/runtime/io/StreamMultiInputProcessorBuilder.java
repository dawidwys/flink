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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;

import javax.annotation.Nonnull;

import java.util.List;

import static org.apache.flink.streaming.runtime.io.EndOfInputUtil.endInput;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class StreamMultiInputProcessorBuilder {

	@Nonnull
	public StreamOneInputProcessor<?>[] createInputProcessors(
			CheckpointedInputGate[] checkpointedInputGates,
			StreamConfig.InputConfig[] configuredInputs,
			IOManager ioManager,
			WatermarkGauge[] inputWatermarkGauges,
			OperatorChain<?, ?> operatorChain,
			List<Input<?>> operatorInputs,
			Counter numRecordsIn,
			MultipleInputSelectionHandler inputSelectionHandler) {
		EndOfInputAwareDataOutput<?>[] dataOutputs = createDataOutputs(
			configuredInputs,
			inputWatermarkGauges,
			operatorInputs,
			numRecordsIn,
			inputSelectionHandler,
			operatorChain
		);
		return createInputProcessors(
			checkpointedInputGates,
			configuredInputs,
			ioManager,
			operatorChain,
			dataOutputs);
	}

	@Nonnull
	private EndOfInputAwareDataOutput<?>[] createDataOutputs(
			StreamConfig.InputConfig[] configuredInputs,
			WatermarkGauge[] inputWatermarkGauges,
			List<Input<?>> operatorInputs,
			Counter numRecordsIn,
			MultipleInputSelectionHandler inputSelectionHandler,
			OperatorChain<?, ?> operatorChain) {
		int inputsCount = configuredInputs.length;
		StreamStatus[] streamStatuses = new StreamStatus[inputsCount];
		EndOfInputAwareDataOutput<?>[] dataOutputs = new EndOfInputAwareDataOutput[inputsCount];
		for (int i = 0; i < inputsCount; i++) {
			streamStatuses[i] = StreamStatus.ACTIVE;
			StreamConfig.InputConfig configuredInput = configuredInputs[i];
			if (configuredInput instanceof StreamConfig.NetworkInputConfig) {
				dataOutputs[i] = new StreamTaskNetworkOutput<>(
					operatorInputs.get(i),
					operatorChain,
					inputWatermarkGauges[i],
					i,
					numRecordsIn,
					inputSelectionHandler,
					streamStatuses);
			}
			else if (configuredInput instanceof StreamConfig.SourceInputConfig) {
				StreamConfig.SourceInputConfig sourceInputConfig = (StreamConfig.SourceInputConfig) configuredInput;
				Output<StreamRecord<?>> chainedSourceOutput = operatorChain.getChainedSourceOutput(sourceInputConfig);
				dataOutputs[i] = createSourceOutput(operatorChain, chainedSourceOutput);
			}
			else {
				throw new UnsupportedOperationException("Unknown input type: " + configuredInput);
			}
		}
		return dataOutputs;
	}

	@Nonnull
	private StreamOneInputProcessor<?>[] createInputProcessors(
			CheckpointedInputGate[] checkpointedInputGates,
			StreamConfig.InputConfig[] configuredInputs,
			IOManager ioManager,
			OperatorChain<?, ?> operatorChain,
			EndOfInputAwareDataOutput<?>[] dataOutputs) {
		int inputsCount = configuredInputs.length;
		StreamOneInputProcessor<?>[] inputProcessors = new StreamOneInputProcessor[inputsCount];
		for (int i = 0; i < inputsCount; i++) {
			StreamConfig.InputConfig configuredInput = configuredInputs[i];
			if (configuredInput instanceof StreamConfig.NetworkInputConfig) {
				StreamConfig.NetworkInputConfig networkInputConfig = (StreamConfig.NetworkInputConfig) configuredInput;
				inputProcessors[i] = createNetworkInputProcessor(
					checkpointedInputGates[(networkInputConfig).getInputGateIndex()],
					ioManager,
					i,
					networkInputConfig,
					dataOutputs[i]);
			}
			else if (configuredInput instanceof StreamConfig.SourceInputConfig) {
				inputProcessors[i] = createSourceInputProcessor(
					operatorChain.getSourceOperator((StreamConfig.SourceInputConfig) configuredInput),
					dataOutputs[i]);
			}
			else {
				throw new UnsupportedOperationException("Unknown input type: " + configuredInput);
			}
		}
		return inputProcessors;
	}

	@Nonnull
	@SuppressWarnings({"rawtypes", "unchecked"})
	private static SourceOperatorStreamTask.AsyncDataOutputToOutput<?> createSourceOutput(
			StreamStatusMaintainer statusMaintainer,
			Output<StreamRecord<?>> chainedSourceOutput) {
		return new SourceOperatorStreamTask.AsyncDataOutputToOutput(chainedSourceOutput, statusMaintainer);
	}

	private static StreamOneInputProcessor<?> createNetworkInputProcessor(
			CheckpointedInputGate checkpointedInputGate,
			IOManager ioManager,
			int inputIdx,
			StreamConfig.NetworkInputConfig configuredInput,
			EndOfInputAwareDataOutput<?> dataOutput) {

		return new StreamOneInputProcessor<>(
			new StreamTaskNetworkInput<>(
				checkpointedInputGate,
				configuredInput.getTypeSerializer(),
				ioManager,
				new StatusWatermarkValve(
					checkpointedInputGate.getNumberOfInputChannels(),
					dataOutput),
				inputIdx),
			dataOutput);
	}

	@Nonnull
	@SuppressWarnings({"unchecked", "rawtypes"})
	private static StreamOneInputProcessor<?> createSourceInputProcessor(
			SourceOperator<?, ?> sourceOperator,
			EndOfInputAwareDataOutput<?> output) {
		return new StreamOneInputProcessor(new StreamTaskSourceInput<>(sourceOperator), output);
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in two input selective processor.
	 */
	private static class StreamTaskNetworkOutput<T> extends AbstractDataOutput<T> {
		private final Input<T> input;

		private final WatermarkGauge inputWatermarkGauge;

		/** The input index to indicate how to process elements by two input operator. */
		private final int inputIndex;

		private final Counter numRecordsIn;
		private final MultipleInputSelectionHandler inputSelectionHandler;

		/**
		 * Stream status for the two inputs. We need to keep track for determining when
		 * to forward stream status changes downstream.
		 */
		private final StreamStatus[] streamStatuses;

		private StreamTaskNetworkOutput(
				Input<T> input,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge inputWatermarkGauge,
				int inputIndex,
				Counter numRecordsIn,
				MultipleInputSelectionHandler inputSelectionHandler,
				StreamStatus[] streamStatuses) {
			super(streamStatusMaintainer);

			this.input = checkNotNull(input);
			this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
			this.inputIndex = inputIndex;
			this.numRecordsIn = numRecordsIn;
			this.inputSelectionHandler = inputSelectionHandler;
			this.streamStatuses = streamStatuses;
		}

		@Override
		public void emitRecord(StreamRecord<T> record) throws Exception {
			input.setKeyContextElement(record);
			input.processElement(record);
			numRecordsIn.inc();
			inputSelectionHandler.nextSelection();
		}

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {
			inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
			input.processWatermark(watermark);
		}

		@Override
		public void emitStreamStatus(StreamStatus streamStatus) {
			streamStatuses[inputIndex] = streamStatus;

			// check if we need to toggle the task's stream status
			if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
				if (streamStatus.isActive()) {
					// we're no longer idle if at least one input has become active
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
				} else if (allStreamStatusesAreIdle()) {
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
				}
			}
		}

		private boolean allStreamStatusesAreIdle() {
			for (StreamStatus streamStatus : streamStatuses) {
				if (streamStatus.isActive()) {
					return false;
				}
			}
			return true;
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			input.processLatencyMarker(latencyMarker);
		}

		@Override
		public void endOutput() throws Exception {
			endInput(input);
		}
	}
}
