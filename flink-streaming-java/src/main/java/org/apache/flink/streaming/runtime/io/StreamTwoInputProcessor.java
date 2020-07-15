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
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.MultipleInputStatusMaintainer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link TwoInputStreamTask}.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public final class StreamTwoInputProcessor<IN1, IN2> implements StreamInputProcessor {

	private final TwoInputSelectionHandler inputSelectionHandler;

	private final OperatorChain<?, ?> operatorChain;
	private final InputOutputBinding<IN1> input1Binding;
	private final InputOutputBinding<IN2> input2Binding;

	/** Input status to keep track for determining whether the input is finished or not. */
	private InputStatus firstInputStatus = InputStatus.MORE_AVAILABLE;
	private InputStatus secondInputStatus = InputStatus.MORE_AVAILABLE;
	/** Always try to read from the first input. */
	private int lastReadInputIndex = 1;

	private boolean isPrepared;

	public StreamTwoInputProcessor(
			CheckpointedInputGate[] checkpointedInputGates,
			Environment runtimeEnvironment,
			StreamConfig configuration,
			AbstractInvokable containingTask,
			IOManager ioManager,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			TwoInputSelectionHandler inputSelectionHandler,
			WatermarkGauge input1WatermarkGauge,
			WatermarkGauge input2WatermarkGauge,
			OperatorChain<?, ?> operatorChain,
			Counter numRecordsIn) {

		this.inputSelectionHandler = checkNotNull(inputSelectionHandler);

		MultipleInputStatusMaintainer streamStatus = new MultipleInputStatusMaintainer(2, streamStatusMaintainer);
		DataOutput<IN1> dataOutput1 = createDataOutput(
			streamStatus.getMaintainerForInput(0),
			streamOperator,
			input1WatermarkGauge,
			record -> processRecord1(record, streamOperator, numRecordsIn),
			0
		);
		DataOutput<IN2> dataOutput2 = createDataOutput(
			streamStatus.getMaintainerForInput(1),
			streamOperator,
			input2WatermarkGauge,
			record -> processRecord2(record, streamOperator, numRecordsIn),
			1
		);

		ClassLoader userClassLoader = runtimeEnvironment.getUserClassLoader();
		TypeSerializer<IN1> inputSerializer1 = configuration.getTypeSerializerIn1(userClassLoader);
		TypeSerializer<IN2> inputSerializer2 = configuration.getTypeSerializerIn2(userClassLoader);
		StreamEdge streamEdge1 = configuration.getInPhysicalEdges(userClassLoader).get(0);
		StreamEdge streamEdge2 = configuration.getInPhysicalEdges(userClassLoader).get(1);
		KeySelector<?, Serializable> keySelector1 = configuration.getStatePartitioner(0, userClassLoader);
		KeySelector<?, Serializable> keySelector2 = configuration.getStatePartitioner(1, userClassLoader);
		TypeComparator<Object> keyComparator = configuration.getStateKeyComparator(userClassLoader);
		boolean sortInput1 =
			keyComparator != null && keySelector1 != null && streamEdge1.getShuffleMode() == ShuffleMode.BATCH;
		boolean sortInput2 =
			keyComparator != null && keySelector2 != null && streamEdge2.getShuffleMode() == ShuffleMode.BATCH;
		if (sortInput1 && sortInput2) {
			MultipleInputSortingDataOutput<Object> combinedOutput = new MultipleInputSortingDataOutput<Object>(
				new DataOutput[]{dataOutput1, dataOutput2},
				runtimeEnvironment,
				new TypeSerializer[]{inputSerializer1, inputSerializer2},
				new KeySelector[]{keySelector1, keySelector2},
				keyComparator,
				containingTask
			);
			dataOutput1 = combinedOutput.getSingleInputOutput(0);
			dataOutput2 = combinedOutput.getSingleInputOutput(1);
		}

		this.input1Binding =
			new InputOutputBinding<>(
				new StreamTaskNetworkInput<>(
					checkpointedInputGates[0],
					inputSerializer1,
					ioManager,
					new StatusWatermarkValve(checkpointedInputGates[0].getNumberOfInputChannels(), dataOutput1),
					0),
				dataOutput1
			);
		this.input2Binding = new InputOutputBinding<>(
			new StreamTaskNetworkInput<>(
				checkpointedInputGates[1],
				inputSerializer2,
				ioManager,
				new StatusWatermarkValve(checkpointedInputGates[1].getNumberOfInputChannels(), dataOutput2),
				1),
			dataOutput2);

		this.operatorChain = checkNotNull(operatorChain);
	}

	private <IN> StreamTaskNetworkOutput<IN> createDataOutput(
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			WatermarkGauge input1WatermarkGauge,
			ThrowingConsumer<StreamRecord<IN>, Exception> recordConsumer,
			int inputIndex) {
		return new StreamTaskNetworkOutput<>(
			streamOperator,
			recordConsumer,
			streamStatusMaintainer,
			input1WatermarkGauge,
			inputIndex);
	}

	private void processRecord1(
			StreamRecord<IN1> record,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			Counter numRecordsIn) throws Exception {

		streamOperator.setKeyContextElement1(record);
		streamOperator.processElement1(record);
		postProcessRecord(numRecordsIn);
	}

	private void processRecord2(
			StreamRecord<IN2> record,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			Counter numRecordsIn) throws Exception {

		streamOperator.setKeyContextElement2(record);
		streamOperator.processElement2(record);
		postProcessRecord(numRecordsIn);
	}

	private void postProcessRecord(Counter numRecordsIn) {
		numRecordsIn.inc();
		inputSelectionHandler.nextSelection();
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (inputSelectionHandler.areAllInputsSelected()) {
			return isAnyInputAvailable();
		} else {
			InputOutputBinding<?> input = (inputSelectionHandler.isFirstInputSelected()) ? input1Binding : input2Binding;
			return input.getAvailableFuture();
		}
	}

	@Override
	public InputStatus processInput() throws Exception {
		int readingInputIndex;
		if (isPrepared) {
			readingInputIndex = selectNextReadingInputIndex();
			assert readingInputIndex != InputSelection.NONE_AVAILABLE;
		} else {
			// the preparations here are not placed in the constructor because all work in it
			// must be executed after all operators are opened.
			readingInputIndex = selectFirstReadingInputIndex();
			if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
				return InputStatus.NOTHING_AVAILABLE;
			}
		}

		lastReadInputIndex = readingInputIndex;

		if (readingInputIndex == 0) {
			firstInputStatus = input1Binding.processInput();
			checkFinished(firstInputStatus, lastReadInputIndex);
		} else {
			secondInputStatus = input2Binding.processInput();
			checkFinished(secondInputStatus, lastReadInputIndex);
		}

		return getInputStatus();
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(
			ChannelStateWriter channelStateWriter,
			long checkpointId) throws IOException {
		return CompletableFuture.allOf(
			input1Binding.prepareSnapshot(channelStateWriter, checkpointId),
			input2Binding.prepareSnapshot(channelStateWriter, checkpointId));
	}

	private int selectFirstReadingInputIndex() throws IOException {
		// Note: the first call to nextSelection () on the operator must be made after this operator
		// is opened to ensure that any changes about the input selection in its open()
		// method take effect.
		inputSelectionHandler.nextSelection();

		isPrepared = true;

		return selectNextReadingInputIndex();
	}

	private void checkFinished(InputStatus status, int inputIndex) throws Exception {
		if (status == InputStatus.END_OF_INPUT) {
			operatorChain.endHeadOperatorInput(getInputId(inputIndex));
			inputSelectionHandler.nextSelection();
		}
	}

	private InputStatus getInputStatus() {
		if (firstInputStatus == InputStatus.END_OF_INPUT && secondInputStatus == InputStatus.END_OF_INPUT) {
			return InputStatus.END_OF_INPUT;
		}

		if (inputSelectionHandler.areAllInputsSelected()) {
			if (firstInputStatus == InputStatus.MORE_AVAILABLE || secondInputStatus == InputStatus.MORE_AVAILABLE) {
				return InputStatus.MORE_AVAILABLE;
			} else {
				return InputStatus.NOTHING_AVAILABLE;
			}
		}

		InputStatus selectedStatus = inputSelectionHandler.isFirstInputSelected() ? firstInputStatus : secondInputStatus;
		InputStatus otherStatus = inputSelectionHandler.isFirstInputSelected() ? secondInputStatus : firstInputStatus;
		return selectedStatus == InputStatus.END_OF_INPUT ? otherStatus : selectedStatus;
	}

	@Override
	public void close() throws IOException {
		IOException ex = null;
		try {
			input1Binding.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		try {
			input2Binding.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		if (ex != null) {
			throw ex;
		}
	}

	private int selectNextReadingInputIndex() throws IOException {
		updateAvailability();
		checkInputSelectionAgainstIsFinished();

		int readingInputIndex = inputSelectionHandler.selectNextInputIndex(lastReadInputIndex);
		if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
			return InputSelection.NONE_AVAILABLE;
		}

		// to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
		// always try to check and set the availability of another input
		if (inputSelectionHandler.shouldSetAvailableForAnotherInput()) {
			checkAndSetAvailable(1 - readingInputIndex);
		}

		return readingInputIndex;
	}

	private void checkInputSelectionAgainstIsFinished() throws IOException {
		if (inputSelectionHandler.areAllInputsSelected()) {
			return;
		}
		if (inputSelectionHandler.isFirstInputSelected() && firstInputStatus == InputStatus.END_OF_INPUT) {
			throw new IOException("Can not make a progress: only first input is selected but it is already finished");
		}
		if (inputSelectionHandler.isSecondInputSelected() && secondInputStatus == InputStatus.END_OF_INPUT) {
			throw new IOException("Can not make a progress: only second input is selected but it is already finished");
		}
	}

	private void updateAvailability() {
		updateAvailability(firstInputStatus, input1Binding);
		updateAvailability(secondInputStatus, input2Binding);
	}

	private void updateAvailability(InputStatus status, InputOutputBinding<?> input) {
		if (status == InputStatus.MORE_AVAILABLE || (status != InputStatus.END_OF_INPUT && input.isApproximatelyAvailable())) {
			inputSelectionHandler.setAvailableInput(input.getInputIndex());
		} else {
			inputSelectionHandler.setUnavailableInput(input.getInputIndex());
		}
	}

	private void checkAndSetAvailable(int inputIndex) {
		InputStatus status = (inputIndex == 0 ? firstInputStatus : secondInputStatus);
		if (status == InputStatus.END_OF_INPUT) {
			return;
		}

		// TODO: isAvailable() can be a costly operation (checking volatile). If one of
		// the input is constantly available and another is not, we will be checking this volatile
		// once per every record. This might be optimized to only check once per processed NetworkBuffer
		if (getInput(inputIndex).isAvailable()) {
			inputSelectionHandler.setAvailableInput(inputIndex);
		}
	}

	private CompletableFuture<?> isAnyInputAvailable() {
		if (firstInputStatus == InputStatus.END_OF_INPUT) {
			return input2Binding.getAvailableFuture();
		}

		if (secondInputStatus == InputStatus.END_OF_INPUT) {
			return input1Binding.getAvailableFuture();
		}

		return (input1Binding.isApproximatelyAvailable() || input2Binding.isApproximatelyAvailable()) ?
			AVAILABLE : CompletableFuture.anyOf(input1Binding.getAvailableFuture(), input2Binding.getAvailableFuture());
	}

	private InputOutputBinding<?> getInput(int inputIndex) {
		return inputIndex == 0 ? input1Binding : input2Binding;
	}

	private int getInputId(int inputIndex) {
		return inputIndex + 1;
	}

	private static final class InputOutputBinding<IN> implements AvailabilityProvider, AutoCloseable {
		private final StreamTaskInput<IN> input;
		private final DataOutput<IN> output;

		private InputOutputBinding(
				StreamTaskInput<IN> input,
				DataOutput<IN> output) {
			this.input = input;
			this.output = output;
		}

		public CompletableFuture<?> getAvailableFuture() {
			return input.getAvailableFuture();
		}

		public InputStatus processInput() throws Exception {
			InputStatus inputStatus = input.emitNext(output);
			if (inputStatus == InputStatus.END_OF_INPUT) {
				output.endOutput();
			}
			return inputStatus;
		}

		public int getInputIndex() {
			return input.getInputIndex();
		}

		public CompletableFuture<?> prepareSnapshot(
				ChannelStateWriter channelStateWriter,
				long checkpointId) throws IOException {
			return input.prepareSnapshot(channelStateWriter, checkpointId);
		}

		public void close() throws IOException {
			input.close();
		}
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in two input selective processor.
	 */
	private class StreamTaskNetworkOutput<T> extends AbstractDataOutput<T> {

		private final TwoInputStreamOperator<IN1, IN2, ?> operator;

		/** The function way is only used for frequent record processing as for JIT optimization. */
		private final ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer;

		private final WatermarkGauge inputWatermarkGauge;

		/** The input index to indicate how to process elements by two input operator. */
		private final int inputIndex;

		private StreamTaskNetworkOutput(
				TwoInputStreamOperator<IN1, IN2, ?> operator,
				ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge inputWatermarkGauge,
				int inputIndex) {
			super(streamStatusMaintainer);

			this.operator = checkNotNull(operator);
			this.recordConsumer = checkNotNull(recordConsumer);
			this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
			this.inputIndex = inputIndex;
		}

		@Override
		public void emitRecord(StreamRecord<T> record) throws Exception {
			recordConsumer.accept(record);
		}

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {
			inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
			if (inputIndex == 0) {
				operator.processWatermark1(watermark);
			} else {
				operator.processWatermark2(watermark);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			if (inputIndex == 0) {
				operator.processLatencyMarker1(latencyMarker);
			} else {
				operator.processLatencyMarker2(latencyMarker);
			}
		}
	}
}
