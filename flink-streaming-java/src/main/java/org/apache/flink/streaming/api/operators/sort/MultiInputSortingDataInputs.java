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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MutableObjectIterator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;

/**
 * An entry class for creating coupled, sorting inputs. The inputs are sorted independently and afterwards
 * the inputs are being merged in order. It is done, by reporting availability only for the input which is
 * current head for the sorted inputs.
 */
public class MultiInputSortingDataInputs<K> {
	private final StreamTaskInput<?>[] inputs;
	private final PushSorter<Tuple2<byte[], StreamRecord<Object>>>[] sorters;
	private final PriorityQueue<HeadElement> queueOfHeads = new PriorityQueue<>();
	private final KeySelector<Object, K>[] keySelectors;
	private final TypeSerializer<K> keySerializer;
	private final DataOutputSerializer dataOutputSerializer;
	private final SortingPhaseDataOutput sortingPhaseDataOutput = new SortingPhaseDataOutput();

	private long notFinishedSortingMask = 0;
	private long finishedEmitting = 0;

	public MultiInputSortingDataInputs(
			StreamTaskInput<Object>[] inputs,
			KeySelector<Object, K>[] keySelectors,
			TypeSerializer<Object>[] inputSerializers,
			TypeSerializer<K> keySerializer,
			MemoryManager memoryManager,
			IOManager ioManager,
			boolean objectReuse,
			double managedMemoryFraction,
			Configuration jobConfiguration,
			AbstractInvokable containingTask) {
		this.inputs = inputs;
		this.keySelectors = keySelectors;
		this.keySerializer = keySerializer;
		int keyLength = keySerializer.getLength();
		final TypeComparator<Tuple2<byte[], StreamRecord<Object>>> comparator;
		if (keyLength > 0) {
			this.dataOutputSerializer = new DataOutputSerializer(keyLength);
			comparator = new FixedLengthByteKeyComparator<>(keyLength);
		} else {
			this.dataOutputSerializer = new DataOutputSerializer(64);
			comparator = new VariableLengthByteKeyComparator<>();
		}
		int numberOfInputs = inputs.length;
		sorters = new PushSorter[numberOfInputs];
		for (int i = 0; i < numberOfInputs; i++) {
			notFinishedSortingMask = setBitMask(notFinishedSortingMask, i);
			KeyAndValueSerializer<Object> keyAndValueSerializer = new KeyAndValueSerializer<>(
				inputSerializers[i],
				keyLength);
			try {
				sorters[i] = ExternalSorter.newBuilder(
					memoryManager,
					containingTask,
					keyAndValueSerializer,
					comparator)
					.memoryFraction(managedMemoryFraction / numberOfInputs)
					.enableSpilling(
						ioManager,
						jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
					.maxNumFileHandles(jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN) / numberOfInputs)
					.objectReuse(objectReuse)
					.largeRecords(true)
					.build();
			} catch (MemoryAllocationException e) {
				throw new RuntimeException();
			}
		}
	}

	public <IN> StreamTaskInput<IN> getInput(int idx) {
		return new IndexedInput<>(idx);
	}

	/**
	 * A thin wrapper that represents a head of a sorted input. Additionally it keeps the id of
	 * the input it belongs to and a future to report availability of more records for the corresponding
	 * input.
	 *
	 * <p>The class is mutable and we only ever have a single instance per input.
	 */
	private static final class HeadElement implements Comparable<HeadElement> {
		Tuple2<byte[], StreamRecord<Object>> streamElement;
		int inputIndex;
		CompletableFuture<Void> notifyInputAvailable;

		@Override
		public int compareTo(HeadElement o) {
			int keyCmp = compare(streamElement.f0, o.streamElement.f0);
			if (keyCmp != 0) {
				return keyCmp;
			}
			return Long.compare(
				streamElement.f1.asRecord().getTimestamp(),
				o.streamElement.f1.asRecord().getTimestamp());
		}

		private int compare(byte[] first, byte[] second) {
			int firstLength = first.length;
			int secondLength = second.length;
			int minLength = Math.min(firstLength, secondLength);
			for (int i = 0; i < minLength; i++) {
				int cmp = Byte.compare(first[i], second[i]);

				if (cmp != 0) {
					return cmp;
				}
			}

			return Integer.compare(firstLength, secondLength);
		}
	}

	/**
	 * An input that wraps an underlying input and sorts the incoming records. It starts emitting records
	 * downstream only when all the other inputs coupled with this {@link MultiInputSortingDataInputs} have
	 * finished sorting as well.
	 *
	 * <p>Moreover it will notify it has some records pending only if the head of the {@link #queueOfHeads}
	 * belongs to the input. In the queue we keep futures for reporting availability. When inserting into
	 * the queue we always notify the head. That way there is only ever one input that reports it is available.
	 */
	private class IndexedInput<IN> implements StreamTaskInput<IN> {

		private final int idx;
		private MutableObjectIterator<Tuple2<byte[], StreamRecord<Object>>> sortedInput;
		private CompletableFuture<Void> headOfQueueFuture;

		private IndexedInput(int idx) {
			this.idx = idx;
		}

		@Override
		public int getInputIndex() {
			return idx;
		}

		@Override
		public CompletableFuture<Void> prepareSnapshot(
				ChannelStateWriter channelStateWriter,
				long checkpointId) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() throws IOException {
			IOException ex = null;
			try {
				inputs[idx].close();
			} catch (IOException e) {
				ex = ExceptionUtils.firstOrSuppressed(e, ex);
			}

			try {
				sorters[idx].close();
			} catch (IOException e) {
				ex = ExceptionUtils.firstOrSuppressed(e, ex);
			}

			if (ex != null) {
				throw ex;
			}
		}

		@Override
		@SuppressWarnings({"rawtypes", "unchecked"})
		public InputStatus emitNext(DataOutput<IN> output) throws Exception {
			if (sortedInput != null) {
				if (checkBitMask(finishedEmitting, idx)) {
					return InputStatus.END_OF_INPUT;
				} else if (headOfQueueFuture.isDone()) {
					HeadElement headElement = queueOfHeads.poll();
					output.emitRecord((StreamRecord<IN>) headElement.streamElement.f1);
					InputStatus inputStatus = addNextToQueue(headElement);
					if (inputStatus == InputStatus.END_OF_INPUT) {
						output.emitWatermark(Watermark.MAX_WATERMARK);
					}
					return inputStatus;
				} else {
					throw new IllegalStateException("The head of queue does not belong to us. The caller" +
						" should've waited until the getAvailableFuture finished.");
				}
			}

			sortingPhaseDataOutput.currentIdx = idx;
			InputStatus inputStatus = inputs[idx].emitNext((DataOutput) sortingPhaseDataOutput);
			if (inputStatus == InputStatus.END_OF_INPUT) {
				return endSorting();
			}

			return inputStatus;
		}

		private InputStatus endSorting() throws Exception {
			sorters[idx].finishReading();
			notFinishedSortingMask = unsetBitMask(notFinishedSortingMask, idx);
			sortedInput = sorters[idx].getIterator();
			return addNextToQueue(new HeadElement());
		}

		@Nonnull
		private InputStatus addNextToQueue(HeadElement reuse) throws IOException {
			Tuple2<byte[], StreamRecord<Object>> next = sortedInput.next();
			if (next != null) {
				reuse.inputIndex = idx;
				reuse.streamElement = next;
				headOfQueueFuture = new CompletableFuture<>();
				reuse.notifyInputAvailable = headOfQueueFuture;
				queueOfHeads.add(reuse);
			}

			if (allSorted()) {
				HeadElement headElement = queueOfHeads.peek();
				if (headElement != null) {
					headElement.notifyInputAvailable.complete(null);
					if (headElement.inputIndex == idx) {
						return InputStatus.MORE_AVAILABLE;
					}
				}
			}

			if (next == null) {
				finishedEmitting = setBitMask(finishedEmitting, idx);
				return InputStatus.END_OF_INPUT;
			} else {
				return InputStatus.NOTHING_AVAILABLE;
			}
		}

		@Override
		public CompletableFuture<?> getAvailableFuture() {
			if (headOfQueueFuture != null) {
				return headOfQueueFuture;
			} else {
				return inputs[idx].getAvailableFuture();
			}
		}
	}

	/**
	 * A simple {@link PushingAsyncDataInput.DataOutput} used in the sorting phase when we have not seen all the
	 * records from the underlying input yet. It forwards the records to a corresponding sorter.
	 */
	private class SortingPhaseDataOutput implements PushingAsyncDataInput.DataOutput<Object> {

		int currentIdx;

		@Override
		public void emitRecord(StreamRecord<Object> streamRecord) throws Exception {
			K key = keySelectors[currentIdx].getKey(streamRecord.getValue());

			keySerializer.serialize(key, dataOutputSerializer);
			byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
			dataOutputSerializer.clear();

			sorters[currentIdx].writeRecord(Tuple2.of(serializedKey, streamRecord));
		}

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {

		}

		@Override
		public void emitStreamStatus(StreamStatus streamStatus) throws Exception {

		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {

		}
	}

	private boolean allSorted() {
		return notFinishedSortingMask == 0;
	}

	private static long setBitMask(long mask, int inputIndex) {
		return mask | 1L << inputIndex;
	}

	private static long unsetBitMask(long mask, int inputIndex) {
		return mask & ~(1L << inputIndex);
	}

	private static boolean checkBitMask(long mask, int inputIndex) {
		return (mask & (1L << inputIndex)) != 0;
	}
}
