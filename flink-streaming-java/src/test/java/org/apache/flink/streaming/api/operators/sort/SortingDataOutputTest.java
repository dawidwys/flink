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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SortingDataOutputTest {
	@Test
	public void simpleFixedLengthKeySorting() throws Exception {
		CollectingDataOutput<Integer> collectingDataOutput = new CollectingDataOutput<>();
		SortingDataOutput<Integer, Integer> sortingDataOutput = new SortingDataOutput<>(
			collectingDataOutput,
			MockEnvironment.builder().build(),
			new IntSerializer(),
			new IntSerializer(),
			(KeySelector<Integer, Integer>) value -> value,
			new DummyInvokable()
		);

		sortingDataOutput.emitRecord(new StreamRecord<>(1, 3));
		sortingDataOutput.emitRecord(new StreamRecord<>(1, 1));
		sortingDataOutput.emitRecord(new StreamRecord<>(2, 1));
		sortingDataOutput.emitRecord(new StreamRecord<>(2, 3));
		sortingDataOutput.emitRecord(new StreamRecord<>(1, 2));
		sortingDataOutput.emitRecord(new StreamRecord<>(2, 2));

		sortingDataOutput.endOutput();

		assertThat(collectingDataOutput.events, equalTo(
			Arrays.asList(
				new StreamRecord<>(1, 1),
				new StreamRecord<>(1, 2),
				new StreamRecord<>(1, 3),
				new StreamRecord<>(2, 1),
				new StreamRecord<>(2, 2),
				new StreamRecord<>(2, 3)
			)
		));
	}

	@Test
	public void simpleVariableLengthKeySorting() throws Exception {
		CollectingDataOutput<Integer> collectingDataOutput = new CollectingDataOutput<>();
		SortingDataOutput<Integer, String> sortingDataOutput = new SortingDataOutput<>(
			collectingDataOutput,
			MockEnvironment.builder().build(),
			new IntSerializer(),
			new StringSerializer(),
			(KeySelector<Integer, String>) value -> "" + value,
			new DummyInvokable()
		);

		sortingDataOutput.emitRecord(new StreamRecord<>(1, 3));
		sortingDataOutput.emitRecord(new StreamRecord<>(1, 1));
		sortingDataOutput.emitRecord(new StreamRecord<>(2, 1));
		sortingDataOutput.emitRecord(new StreamRecord<>(2, 3));
		sortingDataOutput.emitRecord(new StreamRecord<>(1, 2));
		sortingDataOutput.emitRecord(new StreamRecord<>(2, 2));

		sortingDataOutput.endOutput();

		assertThat(collectingDataOutput.events, equalTo(
			Arrays.asList(
				new StreamRecord<>(1, 1),
				new StreamRecord<>(1, 2),
				new StreamRecord<>(1, 3),
				new StreamRecord<>(2, 1),
				new StreamRecord<>(2, 2),
				new StreamRecord<>(2, 3)
			)
		));
	}

	/**
	 * A test utility implementation of {@link PushingAsyncDataInput.DataOutput} that collects all events.
	 */
	private static final class CollectingDataOutput<E> implements PushingAsyncDataInput.DataOutput<E> {

		final List<Object> events = new ArrayList<>();

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {
			events.add(watermark);
		}

		@Override
		public void emitStreamStatus(StreamStatus streamStatus) throws Exception {
			events.add(streamStatus);
		}

		@Override
		public void emitRecord(StreamRecord<E> streamRecord) throws Exception {
			events.add(streamRecord);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			events.add(latencyMarker);
		}
	}
}
