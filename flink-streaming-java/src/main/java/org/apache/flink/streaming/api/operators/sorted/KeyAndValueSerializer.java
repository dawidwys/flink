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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

/**
 * A serializer used in {@link BatchOneInputSortingOperator} for serializing elements alongside their key and
 * timestamp. It serializes the record in a format known by the {@link StreamElementComparator}.
 */
final class KeyAndValueSerializer<IN, KEY> extends TypeSerializer<StreamElement> {
	private final TypeSerializer<IN> valueSerializer;
	private final TypeSerializer<KEY> keySerializer;
	private final KeySelector<IN, KEY> keySelector;

	KeyAndValueSerializer(
		TypeSerializer<IN> valueSerializer,
		TypeSerializer<KEY> keySerializer,
		KeySelector<IN, KEY> keySelector) {
		this.valueSerializer = valueSerializer;
		this.keySerializer = keySerializer;
		this.keySelector = keySelector;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<StreamElement> duplicate() {
		return new KeyAndValueSerializer<>(valueSerializer, keySerializer, keySelector);
	}

	@Override
	public StreamElement createInstance() {
		return new StreamRecord<>(valueSerializer.createInstance());
	}

	@Override
	public StreamElement copy(StreamElement from) {
		StreamRecord<IN> fromRecord = from.asRecord();
		return fromRecord.copy(valueSerializer.copy(fromRecord.getValue()));
	}

	@Override
	public StreamElement copy(StreamElement from, StreamElement reuse) {
		StreamRecord<IN> fromRecord = from.asRecord();
		StreamRecord<IN> reuseRecord = reuse.asRecord();

		IN valueCopy = valueSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
		fromRecord.copyTo(valueCopy, reuseRecord);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StreamElement record, DataOutputView target) throws IOException {
		try {
			StreamRecord<IN> toSerialize = record.asRecord();
			KEY key = keySelector.getKey(toSerialize.getValue());
			keySerializer.serialize(key, target);
			target.writeLong(toSerialize.getTimestamp());
			valueSerializer.serialize(toSerialize.getValue(), target);
		} catch (EOFException eof) {
			throw eof;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public StreamElement deserialize(DataInputView source) throws IOException {
		keySerializer.deserialize(source);
		long timestamp = source.readLong();
		return new StreamRecord<>(valueSerializer.deserialize(source), timestamp);
	}

	@Override
	public StreamElement deserialize(StreamElement reuse, DataInputView source) throws IOException {
		keySerializer.deserialize(source);
		long timestamp = source.readLong();
		IN value = valueSerializer.deserialize(source);
		StreamRecord<IN> reuseRecord = reuse.asRecord();
		reuseRecord.replace(value, timestamp);
		return reuseRecord;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		keySerializer.copy(source, target);
		target.writeLong(source.readLong());
		valueSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		KeyAndValueSerializer<?, ?> that = (KeyAndValueSerializer<?, ?>) o;
		return Objects.equals(valueSerializer, that.valueSerializer) &&
			Objects.equals(keySerializer, that.keySerializer) &&
			Objects.equals(keySelector, that.keySelector);
	}

	@Override
	public int hashCode() {
		return Objects.hash(valueSerializer, keySerializer, keySelector);
	}

	@Override
	public TypeSerializerSnapshot<StreamElement> snapshotConfiguration() {
		return null;
	}
}
