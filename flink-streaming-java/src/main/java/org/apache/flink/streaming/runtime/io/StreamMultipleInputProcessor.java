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
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.api.graph.StreamConfig.InputConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.NetworkInputConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.SourceInputConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
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
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask.AsyncDataOutputToOutput;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.streaming.runtime.io.EndOfInputUtil.endInput;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input processor for {@link MultipleInputStreamOperator}.
 */
@Internal
@SuppressWarnings({"unchecked", "rawtypes"})
public final class StreamMultipleInputProcessor implements StreamInputProcessor {

	private final MultipleInputSelectionHandler inputSelectionHandler;

	private final StreamOneInputProcessor<?>[] inputProcessors;
	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private final StreamStatus[] streamStatuses;

	private final Counter numRecordsIn;

	/** Always try to read from the first input. */
	private int lastReadInputIndex = 1;

	private boolean isPrepared;

	public StreamMultipleInputProcessor(
			CheckpointedInputGate[] checkpointedInputGates,
			InputConfig[] configuredInputs,
			IOManager ioManager,
			StreamStatusMaintainer streamStatusMaintainer,
			MultipleInputStreamOperator<?> mainOperator,
			MultipleInputSelectionHandler inputSelectionHandler,
			WatermarkGauge[] inputWatermarkGauges,
			OperatorChain<?, ?> operatorChain,
			Counter numRecordsIn) {

		this.inputSelectionHandler = checkNotNull(inputSelectionHandler);

		List<Input> operatorInputs = mainOperator.getInputs();
		int inputsCount = operatorInputs.size();

		this.inputProcessors = new StreamOneInputProcessor[inputsCount];
		this.streamStatuses = new StreamStatus[inputsCount];
		this.numRecordsIn = numRecordsIn;

		for (int i = 0; i < inputsCount; i++) {
			InputConfig configuredInput = configuredInputs[i];
			streamStatuses[i] = StreamStatus.ACTIVE;
			if (configuredInput instanceof NetworkInputConfig) {
				NetworkInputConfig networkInput = (NetworkInputConfig) configuredInput;
				StreamTaskNetworkOutput dataOutput = new StreamTaskNetworkOutput<>(
					operatorInputs.get(i),
					streamStatusMaintainer,
					inputWatermarkGauges[i],
					i);

				inputProcessors[i] = new StreamOneInputProcessor(
					new StreamTaskNetworkInput<>(
						checkpointedInputGates[networkInput.getInputGateIndex()],
						networkInput.getTypeSerializer(),
						ioManager,
						new StatusWatermarkValve(
							checkpointedInputGates[networkInput.getInputGateIndex()].getNumberOfInputChannels(),
							dataOutput),
						i),
					dataOutput);
			}
			else if (configuredInput instanceof SourceInputConfig) {
				SourceInputConfig sourceInput = (SourceInputConfig) configuredInput;
				Output<StreamRecord<?>> chainedSourceOutput = operatorChain.getChainedSourceOutput(sourceInput);
				SourceOperator<?, ?> sourceOperator = operatorChain.getSourceOperator(sourceInput);

				inputProcessors[i] = new SourceInputProcessor(
					new AsyncDataOutputToOutput(chainedSourceOutput, operatorChain),
					new StreamTaskSourceInput(sourceOperator));
			}
			else {
				throw new UnsupportedOperationException("Unknown input type: " + configuredInput);
			}

		}
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (inputSelectionHandler.isAnyInputAvailable() || inputSelectionHandler.areAllInputsFinished()) {
			return AVAILABLE;
		}
		final CompletableFuture<?> anyInputAvailable = new CompletableFuture<>();
		for (int i = 0; i < inputProcessors.length; i++) {
			if (!inputSelectionHandler.isInputFinished(i) && inputSelectionHandler.isInputSelected(i)) {
				inputProcessors[i].getAvailableFuture().thenRun(() -> anyInputAvailable.complete(null));
			}
		}
		return anyInputAvailable;
	}

	@Override
	public InputStatus processInput() throws Exception {
		int readingInputIndex;
		if (isPrepared) {
			readingInputIndex = selectNextReadingInputIndex();
		} else {
			// the preparations here are not placed in the constructor because all work in it
			// must be executed after all operators are opened.
			readingInputIndex = selectFirstReadingInputIndex();
		}
		if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
			return InputStatus.NOTHING_AVAILABLE;
		}

		lastReadInputIndex = readingInputIndex;
		InputStatus inputStatus = inputProcessors[readingInputIndex].processInput();
		if (inputStatus == InputStatus.END_OF_INPUT) {
			inputSelectionHandler.nextSelection();
		}
		return inputSelectionHandler.updateStatus(inputStatus, readingInputIndex);
	}

	private int selectFirstReadingInputIndex() {
		// Note: the first call to nextSelection () on the operator must be made after this operator
		// is opened to ensure that any changes about the input selection in its open()
		// method take effect.
		inputSelectionHandler.nextSelection();

		isPrepared = true;

		return selectNextReadingInputIndex();
	}

	@Override
	public void close() throws IOException {
		IOException ex = null;
		for (StreamOneInputProcessor<?> input : inputProcessors) {
			try {
				input.close();
			} catch (IOException e) {
				ex = ExceptionUtils.firstOrSuppressed(e, ex);
			}
		}

		if (ex != null) {
			throw ex;
		}
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(
			ChannelStateWriter channelStateWriter,
			long checkpointId) throws IOException {
		CompletableFuture<?>[] inputFutures = new CompletableFuture[inputProcessors.length];
		for (int index = 0; index < inputFutures.length; index++) {
			inputFutures[index] = inputProcessors[index].prepareSnapshot(channelStateWriter, checkpointId);
		}
		return CompletableFuture.allOf(inputFutures);
	}

	private int selectNextReadingInputIndex() {
		if (!inputSelectionHandler.isAnyInputAvailable()) {
			fullCheckAndSetAvailable();
		}

		int readingInputIndex = inputSelectionHandler.selectNextInputIndex(lastReadInputIndex);
		if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
			return InputSelection.NONE_AVAILABLE;
		}

		// to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
		// always try to check and set the availability of another input
		if (inputSelectionHandler.shouldSetAvailableForAnotherInput()) {
			fullCheckAndSetAvailable();
		}

		return readingInputIndex;
	}

	private void fullCheckAndSetAvailable() {
		for (int i = 0; i < inputProcessors.length; i++) {
			StreamOneInputProcessor<?> inputProcessor = inputProcessors[i];
			// TODO: isAvailable() can be a costly operation (checking volatile). If one of
			// the input is constantly available and another is not, we will be checking this volatile
			// once per every record. This might be optimized to only check once per processed NetworkBuffer
			if (inputProcessor.isApproximatelyAvailable() || inputProcessor.isAvailable()) {
				inputSelectionHandler.setAvailableInput(i);
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

	private static class SourceInputProcessor<T> extends StreamOneInputProcessor<T> {
		public SourceInputProcessor(
				EndOfInputAwareDataOutput<T> dataOutput,
				StreamTaskInput<T> taskInput) {
			super(taskInput, dataOutput);
		}

		@Override
		public void close() throws IOException {
			// SourceOperator is closed via OperatorChain
		}
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in two input selective processor.
	 */
	private class StreamTaskNetworkOutput<T> extends AbstractDataOutput<T> {
		private final Input<T> input;

		private final WatermarkGauge inputWatermarkGauge;

		/** The input index to indicate how to process elements by two input operator. */
		private final int inputIndex;

		private StreamTaskNetworkOutput(
				Input<T> input,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge inputWatermarkGauge,
				int inputIndex) {
			super(streamStatusMaintainer);

			this.input = checkNotNull(input);
			this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
			this.inputIndex = inputIndex;
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
