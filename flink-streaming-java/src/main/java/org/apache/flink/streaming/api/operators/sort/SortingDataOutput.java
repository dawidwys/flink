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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.MutableObjectIterator;

public class SortingDataOutput<T, K> implements PushingAsyncDataInput.DataOutput<T> {

	private final PushSorter<Tuple2<byte[], StreamRecord<T>>> sorter;
	private final PushingAsyncDataInput.DataOutput<T> chained;
	private final KeySelector<T, K> keySelector;
	private final TypeSerializer<K> keySerializer;
	private final DataOutputSerializer dataOutputSerializer;

	public SortingDataOutput(
			PushingAsyncDataInput.DataOutput<T> chained,
			Environment environment,
			TypeSerializer<T> typeSerializer,
			TypeSerializer<K> keySerializer,
			KeySelector<T, K> keySelector,
			AbstractInvokable containingTask) {
		try {
			this.keySelector = keySelector;
			this.keySerializer = keySerializer;
			int keyLength = keySerializer.getLength();
			final TypeComparator<Tuple2<byte[], StreamRecord<T>>> comparator;
			if (keyLength > 0) {
				this.dataOutputSerializer = new DataOutputSerializer(keyLength);
				comparator = new FixedLengthByteKeyComparator<>(keyLength);
			} else {
				this.dataOutputSerializer = new DataOutputSerializer(64);
				comparator = new VariableLengthByteKeyComparator<>();
			}
			KeyAndValueSerializer<T> keyAndValueSerializer = new KeyAndValueSerializer<>(typeSerializer, keyLength);
			this.chained = chained;
			this.sorter = ExternalSorter.newBuilder(
					environment.getMemoryManager(),
					containingTask,
					keyAndValueSerializer,
					comparator)
				.enableSpilling(environment.getIOManager())
				.build();
		} catch (MemoryAllocationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
		K key = keySelector.getKey(streamRecord.getValue());

		this.keySerializer.serialize(key, dataOutputSerializer);
		byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
		dataOutputSerializer.clear();

		this.sorter.writeRecord(Tuple2.of(serializedKey, streamRecord));
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
		Tuple2<byte[], StreamRecord<T>> next;
		MutableObjectIterator<Tuple2<byte[], StreamRecord<T>>> iterator = sorter.getIterator();
		while ((next = iterator.next()) != null) {
			chained.emitRecord(next.f1);
		}
	}
}
