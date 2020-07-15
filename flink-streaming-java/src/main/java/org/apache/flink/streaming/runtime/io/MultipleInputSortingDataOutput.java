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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.operators.sort.v2.ExternalSorter;
import org.apache.flink.runtime.operators.sort.v2.PushSorter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MultipleInputSortingDataOutput<K> {

	private final List<PushSorter<StreamElement>> sorters;
	private final boolean[] finished;
	private final PushingAsyncDataInput.DataOutput<Object>[] chained;
	private final KeySelector<Object, K>[] keySelectors;
	private final TypeComparator<K> keyComparator;

	public MultipleInputSortingDataOutput(
			PushingAsyncDataInput.DataOutput<Object>[] chained,
			Environment environment,
			TypeSerializer<Object>[] typeSerializers,
			KeySelector<Object, K>[] keySelectors,
			TypeComparator<K> keyComparator,
			AbstractInvokable containingTask) {
		this.chained = chained;
		this.keySelectors = keySelectors;
		this.finished = new boolean[chained.length];
		this.keyComparator = keyComparator;
		Arrays.fill(finished, false);
		this.sorters = IntStream.range(0, chained.length)
			.mapToObj(i -> {
				try {
					StreamElementSerializer<?> streamElementSerializer = new StreamElementSerializer<>(typeSerializers[i]);
					StreamElementComparator<?, K> elementComparator = new StreamElementComparator<>(
						keySelectors[i],
						keyComparator,
						streamElementSerializer);
					return ExternalSorter.newBuilder(
						environment.getMemoryManager(),
						containingTask,
						streamElementSerializer,
						elementComparator)
						.memoryFraction(0.7 / chained.length)
						.enableSpilling(environment.getIOManager())
						.build();
				} catch (MemoryAllocationException e) {
					throw new RuntimeException(e);
				}
			})
			.collect(Collectors.toList());
	}

	public <T> PushingAsyncDataInput.DataOutput<T> getSingleInputOutput(int inputIndex) {
		return new SingleInputDataOutput<>(inputIndex);
	}

	@SuppressWarnings("unchecked")
	private void flush() throws Exception {
		if (!allFinished()) {
			return;
		}

		new MergeSortedEmitter(sorters.stream().map(sorter -> {
			try {
				return sorter.getIterator();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}).toArray(MutableObjectIterator[]::new))
			.emit();
	}

	private boolean allFinished() {
		for (boolean b : finished) {
			if (!b) {
				return false;
			}
		}
		return true;
	}

	private final class MergeSortedEmitter {
		private final PriorityQueue<HeadElement> queue;
		private final MutableObjectIterator<StreamElement>[] iterators;

		private MergeSortedEmitter(MutableObjectIterator<StreamElement>[] iterators) {
			this.iterators = iterators;
			queue = new PriorityQueue<>(iterators.length, new Comparator<HeadElement>() {
				@Override
				public int compare(
						HeadElement o1,
						HeadElement o2) {
					try {
						StreamRecord<Object> record1 = o1.streamElement.asRecord();
						StreamRecord<Object> record2 = o2.streamElement.asRecord();
						K key1 = keySelectors[o1.inputIndex].getKey(record1.getValue());
						K key2 = keySelectors[o2.inputIndex].getKey(record2.getValue());

						int compareKey = keyComparator.compare(key1, key2);

						if (compareKey == 0) {
							return Long.compare(record1.getTimestamp(), record2.getTimestamp());
						} else {
							return compareKey;
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}

		public void emit() throws Exception {
			for (int i = 0; i < iterators.length; i++) {
				StreamElement next = iterators[i].next();
				queue.add(new HeadElement(next, i));
			}

			while (!queue.isEmpty()) {
				HeadElement element = queue.poll();
				int inputIndex = element.inputIndex;
				chained[inputIndex].emitRecord(element.streamElement.asRecord());
				MutableObjectIterator<StreamElement> iterator = iterators[inputIndex];
				StreamElement next = iterator.next();
				if (next != null) {
					queue.add(new HeadElement(next, inputIndex));
				}
			}
		}
	}

	private static final class HeadElement {
		private final StreamElement streamElement;
		private final int inputIndex;

		private HeadElement(StreamElement streamElement, int inputIndex) {
			this.streamElement = streamElement;
			this.inputIndex = inputIndex;
		}
	}

	private class SingleInputDataOutput<IN> implements PushingAsyncDataInput.DataOutput<IN> {

		private final int inputIndex;

		private SingleInputDataOutput(int inputIndex) {
			this.inputIndex = inputIndex;
		}

		@Override
		public void emitRecord(StreamRecord<IN> streamRecord) throws Exception {
			sorters.get(inputIndex).writeRecord(streamRecord);
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

		@Override
		public void endOutput() throws Exception {
			sorters.get(inputIndex).finishReading();
			finished[inputIndex] = true;
			flush();
		}
	}

	private static final class StreamElementComparator<IN, KEY> extends TypeComparator<StreamElement> {

		private final TypeComparator<KEY> keyComparator;
		private final TypeSerializer<StreamElement> typeSerializer;
		private final KeySelector<IN, KEY> keySelector;

		private StreamElementComparator(
				KeySelector<IN, KEY> keySelector,
				TypeComparator<KEY> keyComparator,
				TypeSerializer<StreamElement> typeSerializer) {
			this.keySelector = keySelector;
			this.keyComparator = keyComparator;
			this.typeSerializer = typeSerializer;
		}

		@Override
		public int hash(StreamElement record) {
			return keyComparator.hash(getKey(record));
		}

		@Override
		public void setReference(StreamElement toCompare) {
			keyComparator.setReference(getKey(toCompare));
		}

		private KEY getKey(StreamElement toCompare) {
			try {
				StreamRecord<IN> streamRecord = toCompare.asRecord();
				return keySelector.getKey(streamRecord.getValue());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean equalToReference(StreamElement candidate) {
			return keyComparator.equalToReference(getKey(candidate));
		}

		@Override
		public int compareToReference(TypeComparator<StreamElement> referencedComparator) {
			if (referencedComparator instanceof StreamElementComparator) {
				StreamElementComparator<IN, KEY> otherComparator = (StreamElementComparator<IN, KEY>) referencedComparator;
				return keyComparator.compareToReference(otherComparator.keyComparator);
			}

			throw new IllegalArgumentException();
		}

		@Override
		public int compare(StreamElement first, StreamElement second) {
			return keyComparator.compare(getKey(first), getKey(second));
		}

		@Override
		public int compareSerialized(
				DataInputView firstSource,
				DataInputView secondSource) throws IOException {

			StreamElement firstElement = typeSerializer.deserialize(firstSource);
			StreamElement secondElement = typeSerializer.deserialize(secondSource);
			KEY key1 = getKey(firstElement);
			KEY key2 = getKey(secondElement);
			int compareKey = keyComparator.compare(key1, key2);

			if (compareKey == 0) {
				return Long.compare(firstElement.asRecord().getTimestamp(), secondElement.asRecord().getTimestamp());
			} else {
				return compareKey;
			}
		}

		@Override
		public boolean supportsNormalizedKey() {
			return false;
		}

		@Override
		public boolean supportsSerializationWithKeyNormalization() {
			return false;
		}

		@Override
		public int getNormalizeKeyLen() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void putNormalizedKey(
				StreamElement record,
				MemorySegment target,
				int offset,
				int numBytes) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void writeWithKeyNormalization(StreamElement record, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public StreamElement readWithKeyDenormalization(StreamElement reuse, DataInputView source) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean invertNormalizedKey() {
			return false;
		}

		@Override
		public TypeComparator<StreamElement> duplicate() {
			return new StreamElementComparator<>(keySelector, keyComparator.duplicate(), typeSerializer.duplicate());
		}

		@Override
		public int extractKeys(Object record, Object[] target, int index) {
			StreamRecord<IN> record1 = (StreamRecord<IN>) record;
			return keyComparator.extractKeys(record1.getValue(), target, index);
		}

		@Override
		public TypeComparator<?>[] getFlatComparators() {
			return new TypeComparator[0];
		}
	}
}
