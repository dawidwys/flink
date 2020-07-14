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

public class SortingDataOutput<T, K> implements PushingAsyncDataInput.DataOutput<T> {

	private final PushSorter<StreamElement> sorter;
	private final PushingAsyncDataInput.DataOutput<T> chained;

	public SortingDataOutput(
			PushingAsyncDataInput.DataOutput<T> chained,
			Environment environment,
			TypeSerializer<T> typeSerializer,
			KeySelector<T, K> keySelector,
			TypeComparator<K> keyComparator,
			AbstractInvokable containingTask) {
		try {
			StreamElementSerializer<T> streamElementSerializer = new StreamElementSerializer<>(typeSerializer);
			StreamElementComparator<T, K> elementComparator = new StreamElementComparator<>(
				keySelector,
				keyComparator,
				streamElementSerializer);
			this.chained = chained;
			this.sorter = ExternalSorter.newBuilder(
					environment.getMemoryManager(),
					containingTask,
				streamElementSerializer,
					elementComparator)
				.enableSpilling(environment.getIOManager())
				.build();
		} catch (MemoryAllocationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
		this.sorter.writeRecord(streamRecord);
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
		this.sorter.finishReading();
		StreamElement next;
		MutableObjectIterator<StreamElement> iterator = sorter.getIterator();
		while ((next = iterator.next()) != null) {
			chained.emitRecord(next.asRecord());
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
