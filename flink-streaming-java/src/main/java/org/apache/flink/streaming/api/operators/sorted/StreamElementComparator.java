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

package org.apache.flink.streaming.api.operators.sorted;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;

/**
 * A comparator used in {@link BatchOneInputSortingOperator} which compares records keys and timestamps.
 * It uses binary format produced by the {@link KeyAndValueSerializer}.
 */
final class StreamElementComparator<IN, KEY> extends TypeComparator<StreamElement> {
	private final TypeComparator<KEY> keyComparator;
	private final TypeSerializer<KEY> typeSerializer;
	private final KeySelector<IN, KEY> keySelector;

	StreamElementComparator(
			KeySelector<IN, KEY> keySelector,
			TypeComparator<KEY> keyComparator,
			TypeSerializer<KEY> keySerializer) {
		this.keySelector = keySelector;
		this.keyComparator = keyComparator;
		this.typeSerializer = keySerializer;
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

		int compareKey = keyComparator.compareSerialized(firstSource, secondSource);

		if (compareKey == 0) {
			return Long.compare(firstSource.readLong(), secondSource.readLong());
		} else {
			return compareKey;
		}
	}

	@Override
	public boolean supportsNormalizedKey() {
		return keyComparator.supportsNormalizedKey();
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return keyComparator.getNormalizeKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyComparator.isNormalizedKeyPrefixOnly(keyBytes);
	}

	@Override
	public void putNormalizedKey(
		StreamElement record,
		MemorySegment target,
		int offset,
		int numBytes) {
		keyComparator.putNormalizedKey(getKey(record), target, offset, numBytes);
	}

	@Override
	public void writeWithKeyNormalization(StreamElement record, DataOutputView target) {
		throw new UnsupportedOperationException();
	}

	@Override
	public StreamElement readWithKeyDenormalization(StreamElement reuse, DataInputView source) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean invertNormalizedKey() {
		return keyComparator.invertNormalizedKey();
	}

	@Override
	public TypeComparator<StreamElement> duplicate() {
		return new StreamElementComparator<>(keySelector, keyComparator.duplicate(), typeSerializer.duplicate());
	}

	@Override
	@SuppressWarnings("unchecked")
	public int extractKeys(Object record, Object[] target, int index) {
		StreamRecord<IN> record1 = (StreamRecord<IN>) record;
		return keyComparator.extractKeys(record1.getValue(), target, index);
	}

	@Override
	public TypeComparator<?>[] getFlatComparators() {
		return keyComparator.getFlatComparators();
	}
}
