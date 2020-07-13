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
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.DefaultInMemorySorterFactory;
import org.apache.flink.runtime.operators.sort.ExceptionHandler;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.runtime.operators.sort.v2.ExternalSorter;
import org.apache.flink.runtime.operators.sort.v2.PushBasedRecordProducer;
import org.apache.flink.runtime.operators.sort.v2.RecordReader;
import org.apache.flink.runtime.operators.sort.v2.StageRunner;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

public class SortingDataOutput<T> implements PushingAsyncDataInput.DataOutput<T> {

	private final ExternalSorter<StreamElement> sorter;
	private final PushingAsyncDataInput.DataOutput<T> chained;
	private RecordReader<StreamElement> pushBasedRecordProducer;

	public SortingDataOutput(
			PushingAsyncDataInput.DataOutput<T> chained,
			Environment environment,
			TypeSerializerFactory<StreamElement> typeSerializerFactory,
			TypeComparator<T> typeComparator,
			AbstractInvokable containingTask) {
		MemoryManager memoryManager = environment.getMemoryManager();
		try {
			StreamElementComparator<T> elementComparator = new StreamElementComparator<>(typeComparator);
			this.chained = chained;
			this.sorter = new ExternalSorter<>(
				memoryManager,
				memoryManager.allocatePages(containingTask, memoryManager.computeNumberOfPages(0.5)),
				environment.getIOManager(),
				null,
				containingTask,
				typeSerializerFactory,
				elementComparator,
				-1,
				2,
				0.7f,
				false,
				false,
				environment.getExecutionConfig().isObjectReuseEnabled(),
				new DefaultInMemorySorterFactory<>(typeSerializerFactory, elementComparator, 1),
				new ExternalSorter.ReadingStageFactory() {
					@Override
					@SuppressWarnings("unchecked")
					public <E> StageRunner getReadingThread(
						ExceptionHandler<IOException> exceptionHandler,
						MutableObjectIterator<E> reader,
						StageRunner.StageMessageDispatcher<E> dispatcher,
						LargeRecordHandler<E> largeRecordHandler,
						E reuse,
						long startSpillingBytes) {
						SortingDataOutput.this.pushBasedRecordProducer = (RecordReader<StreamElement>) (RecordReader) new RecordReader<>(
							dispatcher,
							largeRecordHandler,
							startSpillingBytes);
						return new PushBasedRecordProducer<>(SortingDataOutput.this.pushBasedRecordProducer);
					}
				}
			);
		} catch (MemoryAllocationException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
		this.pushBasedRecordProducer.writeRecord(streamRecord);
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
		this.pushBasedRecordProducer.finishReading();
		StreamElement next;
		MutableObjectIterator<StreamElement> iterator = sorter.getIterator();
		while ((next = iterator.next()) != null) {
			chained.emitRecord(next.asRecord());
		}
	}

	private static final class StreamElementComparator<T> extends TypeComparator<StreamElement> {

		private final TypeComparator<T> elementComparator;

		private StreamElementComparator(TypeComparator<T> elementComparator) {
			this.elementComparator = elementComparator;
		}

		@Override
		public int hash(StreamElement record) {
			return elementComparator.hash(getValue(record));
		}

		@Override
		public void setReference(StreamElement toCompare) {
			elementComparator.setReference(getValue(toCompare));
		}

		private T getValue(StreamElement toCompare) {
			StreamRecord<T> streamRecord = toCompare.asRecord();
			return streamRecord.getValue();
		}

		@Override
		public boolean equalToReference(StreamElement candidate) {
			return elementComparator.equalToReference(getValue(candidate));
		}

		@Override
		public int compareToReference(TypeComparator<StreamElement> referencedComparator) {
			if (referencedComparator instanceof StreamElementComparator) {
				StreamElementComparator<T> otherComparator = (StreamElementComparator<T>) referencedComparator;
				return elementComparator.compareToReference(otherComparator.elementComparator);
			}

			throw new IllegalArgumentException();
		}

		@Override
		public int compare(StreamElement first, StreamElement second) {
			return elementComparator.compare(getValue(first), getValue(second));
		}

		@Override
		public int compareSerialized(
				DataInputView firstSource,
				DataInputView secondSource) throws IOException {

			skipTimestamp(firstSource);
			skipTimestamp(secondSource);
			return elementComparator.compareSerialized(firstSource, secondSource);
		}

		private void skipTimestamp(DataInputView serialized) throws IOException {
			int tag = serialized.readByte();
			if (tag == 0) {
				serialized.readLong();
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
			return elementComparator.getNormalizeKeyLen();
		}

		@Override
		public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
			return elementComparator.isNormalizedKeyPrefixOnly(keyBytes);
		}

		@Override
		public void putNormalizedKey(
				StreamElement record,
				MemorySegment target,
				int offset,
				int numBytes) {
			elementComparator.putNormalizedKey(getValue(record), target, offset, numBytes);
		}

		@Override
		public void writeWithKeyNormalization(StreamElement record, DataOutputView target) throws IOException {
			elementComparator.writeWithKeyNormalization(getValue(record), target);
		}

		@Override
		public StreamElement readWithKeyDenormalization(StreamElement reuse, DataInputView source) throws IOException {
			return new StreamRecord<T>(elementComparator.readWithKeyDenormalization(null, source));
		}

		@Override
		public boolean invertNormalizedKey() {
			return elementComparator.invertNormalizedKey();
		}

		@Override
		public TypeComparator<StreamElement> duplicate() {
			return new StreamElementComparator<>(elementComparator.duplicate());
		}

		@Override
		public int extractKeys(Object record, Object[] target, int index) {
			StreamRecord<T> record1 = (StreamRecord<T>) record;
			return elementComparator.extractKeys(record1.getValue(), target, index);
		}

		@Override
		public TypeComparator<?>[] getFlatComparators() {
			return new TypeComparator[0];
		}
	}
}
