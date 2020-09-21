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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Arrays;

/**
 * A comparator used in {@link SortingDataOutput} which compares records keys and timestamps.
 * It uses binary format produced by the {@link KeyAndValueSerializer}.
 */
final class FixedLengthByteKeyComparator<IN> extends TypeComparator<Tuple2<byte[], StreamRecord<IN>>> {
	private static final int TIMESTAMP_BYTE_SIZE = 8;
	private final int keyLength;
	private byte[] keyReference;
	private long timestampReference;

	FixedLengthByteKeyComparator(int keyLength) {
		this.keyLength = keyLength;
	}

	@Override
	public int hash(Tuple2<byte[], StreamRecord<IN>> record) {
		return record.hashCode();
	}

	@Override
	public void setReference(Tuple2<byte[], StreamRecord<IN>> toCompare) {
		this.keyReference = toCompare.f0;
		this.timestampReference = toCompare.f1.asRecord().getTimestamp();
	}

	@Override
	public boolean equalToReference(Tuple2<byte[], StreamRecord<IN>> candidate) {
		return Arrays.equals(keyReference, candidate.f0) &&
			timestampReference == candidate.f1.asRecord().getTimestamp();
	}

	@Override
	public int compareToReference(TypeComparator<Tuple2<byte[], StreamRecord<IN>>> referencedComparator) {
		byte[] otherKey = ((FixedLengthByteKeyComparator<IN>) referencedComparator).keyReference;
		long otherTimestamp = ((FixedLengthByteKeyComparator<IN>) referencedComparator).timestampReference;

		int keyCmp = compare(this.keyReference, otherKey);
		if (keyCmp != 0) {
			return keyCmp;
		}
		return Long.compare(this.timestampReference, otherTimestamp);
	}

	@Override
	public int compare(
			Tuple2<byte[], StreamRecord<IN>> first,
			Tuple2<byte[], StreamRecord<IN>> second) {
		int keyCmp = compare(first.f0, second.f0);
		if (keyCmp != 0) {
			return keyCmp;
		}
		return Long.compare(first.f1.asRecord().getTimestamp(), second.f1.asRecord().getTimestamp());
	}

	private int compare(byte[] first, byte[] second) {
		for (int i = 0; i < keyLength; i++) {
			int cmp = Byte.compare(first[i], second[i]);

			if (cmp != 0) {
				return cmp;
			}
		}

		return 0;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		int minCount = keyLength;
		while (minCount-- > 0) {
			byte firstValue = firstSource.readByte();
			byte secondValue = secondSource.readByte();

			int cmp = Byte.compare(firstValue, secondValue);
			if (cmp != 0) {
				return cmp;
			}
		}

		return Long.compare(firstSource.readLong(), secondSource.readLong());
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return keyLength + TIMESTAMP_BYTE_SIZE;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(Tuple2<byte[], StreamRecord<IN>> record, MemorySegment target, int offset, int numBytes) {
		byte[] data = record.f0;
		int length = this.keyLength;

		if (length >= numBytes) {
			target.put(offset, data, 0, numBytes);
		} else {
			target.put(offset, data, 0, length);
			int lastOffset = offset + numBytes;
			long valueOfTimestamp = record.f1.asRecord().getTimestamp() - Long.MIN_VALUE;
			if (length + TIMESTAMP_BYTE_SIZE > numBytes) {
				for (int i = length; i < lastOffset; i++) {
					target.put(offset + i, (byte) (valueOfTimestamp >>> ((7 - i) << 3)));
				}
			} else {
				offset += length;
				target.putLongBigEndian(offset, valueOfTimestamp);
				offset += TIMESTAMP_BYTE_SIZE;
				while (offset < lastOffset) {
					target.put(offset++, (byte) 0);
				}
			}
		}
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public TypeComparator<Tuple2<byte[], StreamRecord<IN>>> duplicate() {
		return new FixedLengthByteKeyComparator<>(this.keyLength);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = record;
		return 1;
	}

	@Override
	public TypeComparator<?>[] getFlatComparators() {
		return new TypeComparator[] {this};
	}

	// --------------------------------------------------------------------------------------------
	// unsupported normalization
	// --------------------------------------------------------------------------------------------

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(
		Tuple2<byte[], StreamRecord<IN>> record,
		DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Tuple2<byte[], StreamRecord<IN>> readWithKeyDenormalization(
		Tuple2<byte[], StreamRecord<IN>> reuse,
		DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
}
